"""Single supervised worker, durable controls, and shared real/virtual candidate flow."""
import copy,json,os,secrets,time,uuid
from pathlib import Path
from aegis.domain import Blocked,D
from aegis.intelligence.store import ResearchStore,encode
from aegis.intelligence.service import IntelligenceService
from aegis.intelligence.control import ConsoleControl
from aegis.intelligence.connection import PlanConnection
from aegis.intelligence.chart import charts
from aegis.chatgpt import ChatGPT
from aegis.cloud_secrets import CloudVault
from aegis.live_bridge import LiveBridge

DEFAULT_CONTROL={'desired':'RUNNING','mode':'virtual','auto_resume':True,'real_authorized':False}

def open_store():
    if os.environ.get('DATABASE_URL'):
        from aegis.cloud_store import PostgresStore
        return PostgresStore(os.environ['DATABASE_URL'])
    if os.environ.get('AEGIS_LOCAL_WORKSPACE'):
        store=ResearchStore(os.environ['AEGIS_LOCAL_WORKSPACE'])
        with store.db:store.db.execute('CREATE TABLE IF NOT EXISTS cloud_commands(id TEXT PRIMARY KEY,created_at REAL NOT NULL,status TEXT NOT NULL,body TEXT NOT NULL,result TEXT)')
        return store
    raise RuntimeError('DATABASE_URL is required; ephemeral storage cannot hold trading records')

ALLOWED={'control/start','control/pause','control/stop','wallet/add','model/models','model/select','model/disable'}
def enqueue(store,path,body):
    if path not in ALLOWED:raise Blocked('Unsupported control')
    rid=body.get('request_id') or str(uuid.uuid4())
    if not isinstance(rid,str) or not 1<=len(rid)<=100:raise Blocked('Invalid request ID')
    clean={k:v for k,v in body.items() if k in ('mode','confirmation','amount','model')}
    if path=='wallet/add' and (isinstance(clean.get('amount'),bool) or not D(0)<D(clean.get('amount',0))<=D(100000)):raise Blocked('Add between 0.01 and 100000 virtual USDT')
    with store.lock,store.db:
        old=store.db.execute('SELECT body FROM cloud_commands WHERE id=?',(rid,)).fetchone()
        message=encode({'path':path,'body':clean})
        if old and old[0]!=message:raise Blocked('Request ID was already used for another control')
        store.db.execute('INSERT OR IGNORE INTO cloud_commands VALUES(?,?,?,?,?)',(rid,time.time(),'PENDING',message,None))
    return {'accepted':True,'command_id':rid}

def command_rows(store,limit=20):
    with store.lock:
        rows=store.db.execute('SELECT * FROM cloud_commands ORDER BY created_at DESC LIMIT ?',(limit,)).fetchall()
    return [{'id':r['id'],'at':r['created_at'],'status':r['status'],'result':json.loads(r['result']) if r['result'] else None} for r in rows]

class CloudConnection(PlanConnection):
    def __init__(self,service,vault):
        self.service=service;self.vault=vault;self.chat=ChatGPT(vault,0);self.error=None;self.restore()
    def saved(self):raise Blocked('Cloud connection requires the one-time local sign-in helper')
    def import_profile(self,data):
        # The official loopback helper validates the nonce/identity. Verify the
        # signed identity again at the receiving host before accepting credentials.
        import jwt
        profile=data.get('profile',{});client=profile.get('client_id');token=profile.get('id_token')
        if not isinstance(client,str) or not token or not profile.get('refresh_token'):raise Blocked('Incomplete ChatGPT sign-in')
        discovery=self.chat.oidc();key=jwt.PyJWKClient(discovery['jwks_uri'],timeout=8).get_signing_key_from_jwt(token)
        claims=jwt.decode(token,key.key,algorithms=['RS256','ES256'],audience=client,issuer=discovery['issuer'],leeway=60,options={'require':['exp','iss','aud','sub']})
        if claims['sub']!=profile.get('sub') or 'chatgpt.tokens.use.direct' not in profile.get('scopes',[]):raise Blocked('ChatGPT plan permission or identity is invalid')
        allowed={'client_id','sub','email','name','id_token','access_token','refresh_token','scopes','expires_at','issued_at'}
        pid=data.get('active')
        if not isinstance(pid,str) or not 1<=len(pid)<=100:raise Blocked('Invalid account registration')
        self.vault.update('chatgpt',{'active':pid,'profiles':{pid:{k:v for k,v in profile.items() if k in allowed}}})
        self.catalog();return {'connected':True}

class Supervisor:
    def __init__(self,service,connection,live):
        self.service=service;self.connection=connection;self.live=live;self.store=service.store
        self.control=self.store.state('cloud_control',copy.deepcopy(DEFAULT_CONTROL))
        self.store.put_state('cloud_control',self.control);self.retry_at=0;self.retries=0;self.stable_since=None
        self.connection_identity=None;self.connection_check=0
        service.execution_mode=self.control['mode'];service.live=live
        service.spot.active=lambda:live if service.execution_mode=='real' else service.spot.paper
        with self.store.lock,self.store.db:
            self.store.db.execute("UPDATE cloud_commands SET status='INTERRUPTED',result=? WHERE status='PROCESSING'",(encode({'error':'Worker restarted during this control; inspect state before retrying'}),))
    def save_control(self):self.store.put_state('cloud_control',self.control)
    def handle(self,path,body):
        s=self.service
        if path=='control/start':
            mode=body.get('mode',self.control['mode'])
            if mode not in ('virtual','real'):raise Blocked('Choose virtual or real')
            if mode!=s.execution_mode and (s.spot.paper.state['position'] or self.live.state['position'] or self.live.state.get('pending_entry') or self.live.broker.unresolved()):raise Blocked('Close or reconcile the managed position/orders before switching modes')
            if mode=='real':self.live.arm(body.get('confirmation'))
            s.pause();s.execution_mode=mode;self.control.update(desired='RUNNING',mode=mode,real_authorized=mode=='real');self.save_control()
            self.retry_at=0;self.retries=0;s.control.start('paper')
            return {'message':'Starting '+mode+' market checks'}
        if path in ('control/pause','control/stop'):
            self.control['desired']='STOPPED' if path.endswith('stop') else 'PAUSED';self.save_control()
            s.control.pause(emergency=path.endswith('stop'));self.live.pause('New real entries paused; native protection remains on Binance')
            return {'message':'New entries stopped; existing positions remain managed'}
        if path=='wallet/add':
            if s.execution_mode!='virtual':raise Blocked('Virtual USDT can only be added in virtual mode')
            if s.paper_mode=='RUNNING' or s.control.snapshot()['busy'] or s.spot.paper.state['position']:raise Blocked('Pause entries and wait for the virtual position to close before adding funds')
            amount=D(body['amount']);p=s.spot.paper
            with self.store.lock,self.store.db:
                p.state['cash']=str(D(p.state['cash'])+amount);p.state['initial']=str(D(p.state['initial'])+amount)
                p.state['peak_bot_equity']=str(D(p.state['peak_bot_equity'])+amount);p.settings['allocation']=float(p.state['initial'])
                p.state['capital_flows']=p.state.get('capital_flows',[])+[{'at':s.market_now(),'amount':str(amount)}]
                # Start a new equity segment after a transfer so deposits cannot
                # masquerade as trading returns or create artificial drawdown.
                p.state['equity']=[];p._commit();p.mark(next(iter(p.bids.values()),1),s.market_now())
                self.store.append('observations',{'kind':'VIRTUAL_DEPOSIT','amount':str(amount),'wallet_key':p.key},version='wallet-transfer-v1')
            return {'message':'Added '+str(amount)+' virtual USDT; real balances unchanged'}
        if path=='model/models':return self.connection.catalog()
        if path=='model/select':return self.connection.select(body.get('model'))
        if path=='model/disable':self.connection.disable();self.control['desired']='PAUSED';self.save_control();return {'message':'GPT disabled; entries paused'}
        raise Blocked('Unknown control')
    def commands(self):
        with self.store.lock:rows=self.store.db.execute("SELECT * FROM cloud_commands WHERE status='PENDING' ORDER BY created_at LIMIT 10").fetchall()
        for row in rows:
            with self.store.lock,self.store.db:self.store.db.execute("UPDATE cloud_commands SET status='PROCESSING' WHERE id=? AND status='PENDING'",(row['id'],))
            try:
                value=json.loads(row['body']);result=self.handle(value['path'],value['body']);status='COMPLETED'
            except Exception as exc:result={'error':str(exc) if isinstance(exc,(Blocked,ValueError)) else 'Control failed; inspect worker logs'};status='FAILED'
            with self.store.lock,self.store.db:self.store.db.execute('UPDATE cloud_commands SET status=?,result=? WHERE id=?',(status,encode(result),row['id']))
    def tick(self):
        s=self.service;self.commands()
        if time.monotonic()-self.connection_check>5:
            self.connection_check=time.monotonic();saved=self.connection.chat.saved();active=saved.get('active');profile=saved.get('profiles',{}).get(active,{})
            signature=(active,profile.get('client_id'),profile.get('sub'),self.store.state('spot_selected_model'),self.store.state('spot_model_authorized'))
            if signature!=self.connection_identity:
                s.pause();self.connection.restore();self.connection_identity=signature
                if not signature[-1]:s.spot.analyst.economy.enabled=False
        self.live.poll()
        if self.control['desired']!='RUNNING':return
        if s.paper_mode=='RUNNING':
            if s.execution_mode=='real' and self.control['real_authorized'] and self.live.state['mode']=='PAUSED' and not self.live.fault and not self.live.broker.unresolved() and s.control.fresh():
                self.live.change(lambda x:x.update(mode='RUNNING',generation=x['generation']+1))
            self.retries=0;self.stable_since=None;return
        if s.control.snapshot()['busy'] or time.monotonic()<self.retry_at:return
        stream=getattr(s,'stream',None)
        if stream:
            if not s.control.fresh():self.stable_since=None;return
            if self.stable_since is None:self.stable_since=time.monotonic();return
            if time.monotonic()-self.stable_since<3:return
        if s.execution_mode=='real':
            if not self.control['real_authorized'] or self.live.fault or self.live.broker.unresolved():return
            # Auto-resume uses the owner's persisted real-mode authorization.
            self.live.change(lambda x:x.update(mode='RUNNING',generation=x['generation']+1))
        s.control.start('paper');self.retries+=1;self.retry_at=time.monotonic()+min(60,5*2**min(self.retries,4))

def view(service,supervisor):
    s=service;v=s.snapshot();live=supervisor.live;real=s.execution_mode=='real';broker=live if real else s.spot.paper
    if real:
        state=live.view_state();v.update(paper=live.metrics(),paper_key=live.key,paper_cash=state['cash'],paper_initial=state['initial'],equity_curve=state['equity'])
    frames={}
    episodes=s.store.records('trade_episodes',limit=2000)
    for name,position,key in [('virtual',s.spot.paper.state['position'],s.spot.paper.key),('comparison',s.spot.baseline.state['position'],s.spot.baseline.key),('real',live.view_state()['position'],live.key)]:
        frames[name]=charts(s.prepared,dict(s.chart_previews),episodes,position,s.market_now(),wallet_key=key)
    v.update(cloud=True,execution_mode=s.execution_mode,desired_state=supervisor.control['desired'],
             console=s.control.snapshot(),chatgpt_connection=supervisor.connection.status(),live=live.metrics(),
             worker={'heartbeat_at':time.time(),'auto_resume':True,'restart_attempts':supervisor.retries},
             trade_episodes=episodes,chart_wallets=frames,market_charts=frames['real' if real else 'virtual'],
             observations=s.store.records('observations',limit=100),
             last_commands=command_rows(s.store),virtual_cash=s.spot.paper.state['cash'])
    if s.paper_mode=='RUNNING':v['console']['message']='Scanning fixed Spot setups. GPT assesses feasible candidates; '+('approved entries use actual Binance orders and confirmed fills.' if real else 'fills use the virtual USDT ledger, with separate comparison trades.')
    elif not v['console']['busy'] and supervisor.control['desired']!='RUNNING':v['console']['message']='Entries are paused by your control. Existing positions retain exit management.'
    return v

def build_runtime(store,encryption_key,allow_live=False,allocation=10):
    service=IntelligenceService(store,spot_lab=True);service.restore();service.control=ConsoleControl(service)
    service.spot.paper.settings['allocation']=float(service.spot.paper.state['initial'])
    vault=CloudVault(store,encryption_key);connection=CloudConnection(service,vault);service.connection=connection
    live=LiveBridge(service,vault,allocation,enabled=allow_live)
    original_pause=service.pause
    def pause():
        original_pause();live.pause('New real entries paused; confirmed exchange protection remains active')
    service.pause=pause
    supervisor=Supervisor(service,connection,live)
    # Existing local virtual positions continue deterministic exit management.
    service.spot.resume_exits()
    return service,supervisor
