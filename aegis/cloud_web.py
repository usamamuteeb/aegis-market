"""Owner-only WSGI API. HTTP requests never start an in-process trading worker."""
import html,json,os,secrets,time
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs,urlsplit
from urllib.request import Request,build_opener
from aegis.cloud_auth import Login
from aegis.cloud_runtime import open_store,enqueue,command_rows,CloudConnection
from aegis.cloud_secrets import CloudVault
from aegis.intelligence.spot_model import SpotAnalyst
from aegis.binance import Binance
from aegis.domain import Blocked
from aegis.transport import RemoteError,NoRedirect

ROOT=Path(__file__).resolve().parent.parent
LOGIN='''<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><title>Aegis login</title><style>body{font:16px Segoe UI,sans-serif;background:#f4f7f7;color:#18313b;min-height:95vh;display:grid;place-items:center;margin:0}main{background:white;border:1px solid #dde8e7;padding:40px;border-radius:20px;width:min(340px,80vw)}h1{margin:8px 0}p{color:#6f838b;line-height:1.7}label{display:block;margin-top:20px;font-size:13px}input,button{box-sizing:border-box;width:100%;padding:13px;margin-top:8px;border:1px solid #d9e4e4;border-radius:8px;font:inherit}button{background:#087e67;color:white;cursor:pointer;margin-top:28px}b{color:#087e67}.error{color:#af4545}</style></head><body><main><b>AEGIS</b><h1>Your trading workspace</h1><p>Sign in to view your wallet and control the agent.</p>ERROR<form method="post" action="/login"><label>Username<input name="username" autocomplete="username" required autofocus></label><label>Password<input name="password" type="password" autocomplete="current-password" required></label><button>Sign in</button></form></main></body></html>'''

_store=None
_frontend_opener=build_opener(NoRedirect)

def dashboard_document():
    """Read the independently built UI through the caller's runtime binding."""
    base=os.environ.get('AEGIS_FRONTEND_URL','')
    if base:
        target=urlsplit(base)
        if target.scheme not in ('http','https') or not target.hostname or target.username or target.password or target.query or target.fragment:
            raise RuntimeError('Invalid frontend service binding')
        # Preserve any binding path prefix. Never send owner cookies, tokens or
        # credentials to the static service, and never follow a service redirect.
        request=Request(base.rstrip('/')+'/',headers={'Accept':'text/html'})
        with _frontend_opener.open(request,timeout=8) as response:
            if response.status!=200 or response.headers.get_content_type()!='text/html':
                raise RuntimeError('Frontend service did not return the dashboard')
            raw=response.read(1048577)
        if len(raw)>1048576:raise RuntimeError('Frontend document is too large')
        source=raw.decode('utf-8')
    else:
        if os.environ.get('VERCEL'):
            raise RuntimeError('Frontend service binding is missing')
        source=(ROOT/'frontend/dist/index.html').read_text(encoding='utf-8')
    if '<head>' not in source or '__AEGIS_SESSION__' not in source:
        raise RuntimeError('Frontend document is incompatible')
    return source
def database():
    global _store
    if _store is None:_store=open_store()
    return _store

def application(env,start_response):
    global _store
    def respond(status,body,kind='application/json',headers=()):
        raw=json.dumps(body,allow_nan=False).encode() if isinstance(body,(dict,list)) else body.encode() if isinstance(body,str) else body
        start_response(status,[('Content-Type',kind),('Content-Length',str(len(raw))),('Cache-Control','no-store'),
          ('X-Content-Type-Options','nosniff'),('Referrer-Policy','no-referrer'),
          ('Content-Security-Policy',"default-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; form-action 'self'"),*headers]);return [raw]
    def redirect(where,headers=()):return respond('303 See Other','',headers=[('Location',where),*headers])
    try:
        auth=Login(os.environ.get('AEGIS_USERNAME','aegis'),os.environ.get('AEGIS_PASSWORD',''),os.environ.get('AEGIS_SESSION_SECRET',''),secure=not bool(os.environ.get('AEGIS_LOCAL_WORKSPACE')))
        path=env.get('PATH_INFO','/');method=env.get('REQUEST_METHOD','GET');headers={'Cookie':env.get('HTTP_COOKIE','')};session=auth.valid(headers)
        routed=parse_qs(env.get('QUERY_STRING','')).get('route',[None])[0]
        if path=='/api/index.py' and routed:path='/' if routed=='dashboard' else '/'+routed if routed in ('login','logout') else '/api/'+routed
        if method=='POST':
            size=int(env.get('CONTENT_LENGTH') or 0)
            if not 0<size<=65536:raise Blocked('Invalid request size')
            scheme='https' if os.environ.get('VERCEL') else env.get('wsgi.url_scheme','https')
            origin=env.get('HTTP_ORIGIN');expected=scheme+'://'+env.get('HTTP_HOST','')
            # Embedded browsers may use an opaque/null origin. Every control
            # still requires the owner's signed cookie AND session CSRF token;
            # login still requires the single owner's actual credentials.
            if origin and origin not in (expected,'null'):raise Blocked('Foreign origin rejected')
            raw=env['wsgi.input'].read(size)
            if path=='/login':
                values=parse_qs(raw.decode());username=values.get('username',[''])[0];password=values.get('password',[''])[0]
                store=database();key='login_attempts';now=time.time()
                with store.lock,store.db:
                    attempts=[x for x in store.state(key,[]) if now-x<300]
                    if len(attempts)>=10:return respond('429 Too Many Requests',LOGIN.replace('ERROR','<p class="error">Too many attempts. Wait five minutes.</p>'),'text/html')
                    if not auth.credentials(username,password):
                        store.put_state(key,attempts+[now]);return respond('401 Unauthorized',LOGIN.replace('ERROR','<p class="error">Username or password is incorrect.</p>'),'text/html')
                    store.put_state(key,[])
                return redirect('/',[('Set-Cookie',auth.cookie(auth.token()))])
        if not session:
            if path.startswith('/api/'):return respond('401 Unauthorized',{'error':'Sign in to Aegis'})
            return respond('200 OK',LOGIN.replace('ERROR',''),'text/html')
        if method=='POST' and not secrets.compare_digest(env.get('HTTP_X_AEGIS_TOKEN',''),auth.csrf(session)):raise Blocked('Session expired; refresh and retry')
        if path=='/logout' and method=='POST':return redirect('/login',[('Set-Cookie','aegis_owner=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0')])
        if path in ('/','/login','/api/index.py') and method=='GET':
            source=dashboard_document()
            source=source.replace('__AEGIS_SESSION__',html.escape(auth.csrf(session),quote=True)).replace('<head>','<head><meta name="aegis-cloud" content="1">')
            return respond('200 OK',source,'text/html')
        store=database()
        if path in ('/api/state','/api/intelligence') and method=='GET':
            state=store.state('cloud_snapshot')
            if not state:return respond('200 OK',{'initializing':True,'message':'Waiting for the cloud worker to publish its first heartbeat','last_commands':command_rows(store)})
            state['worker']['stale']=time.time()-state['worker']['heartbeat_at']>30
            state['last_commands']=command_rows(store);return respond('200 OK',state)
        if path=='/api/history' and method=='GET':
            query=parse_qs(env.get('QUERY_STRING',''));before=float(query.get('before',[time.time()+60])[0]);wallet=query.get('wallet',[None])[0]
            rows=store.records('trade_episodes',at=before,limit=500)
            if wallet:rows=[r for r in rows if r['body'].get('wallet_key')==wallet]
            return respond('200 OK',{'trades':rows,'next_before':min((r['as_of'] for r in rows),default=None)})
        if method=='POST' and path.startswith('/api/'):
            body=json.loads(raw)
            if not isinstance(body,dict):raise Blocked('Expected an object')
            route=path[5:]
            if route=='setup/host':
                vault=CloudVault(store,os.environ['AEGIS_ENCRYPTION_KEY']);host=vault.read().get('host_id')
                if not host:
                    import uuid
                    host='urn:uuid:'+str(uuid.uuid4());vault.update('host_id',host)
                return respond('200 OK',{'host_id':host})
            if route=='model/import':
                snapshot=store.state('cloud_snapshot',{})
                if snapshot.get('chatgpt_connection',{}).get('connected') and (snapshot.get('mode')=='RUNNING' or snapshot.get('spot_lab',{}).get('assessment_pending')):raise Blocked('Pause entries and wait for the current assessment before replacing the connected account')
                dummy=SimpleNamespace(store=store,spot=SimpleNamespace(analyst=SpotAnalyst(store,time.time)),pause=lambda:None)
                conn=CloudConnection(dummy,CloudVault(store,os.environ['AEGIS_ENCRYPTION_KEY']))
                conn.import_profile(body)
                return respond('200 OK',{'connected':True,'message':'ChatGPT connected. Worker will load the saved account automatically.'})
            if route=='binance/connect':
                snapshot=store.state('cloud_snapshot',{})
                if snapshot.get('execution_mode')=='real' or snapshot.get('live',{}).get('open_position') or snapshot.get('live',{}).get('pending_entry') or snapshot.get('live',{}).get('unresolved_orders') or snapshot and time.time()-snapshot.get('worker',{}).get('heartbeat_at',0)>30:raise Blocked('Switch to virtual mode and resolve real exposure before changing Binance credentials; the worker must be online')
                vault=CloudVault(store,os.environ['AEGIS_ENCRYPTION_KEY']);api=Binance(vault)
                account=api.connect(body.get('key'),body.get('secret'),'live')
                return respond('200 OK',{'connected':True,'can_trade':bool(account.get('canTrade'))})
            return respond('202 Accepted',enqueue(store,route,body))
        return respond('404 Not Found',{'error':'Not found'})
    except RemoteError as exc:return respond('400 Bad Request',{'error':'Binance rejected the API key, allowed IP or permissions (-2015)' if exc.code==-2015 else str(exc)})
    except (Blocked,ValueError,TypeError,json.JSONDecodeError) as exc:return respond('400 Bad Request',{'error':str(exc)})
    except Exception:
        failed=_store;_store=None
        if failed:
            try:failed.close()
            except Exception:pass
        return respond('503 Service Unavailable',{'error':'Cloud service unavailable. Check database and hosting environment configuration; the next request will reconnect.'})
