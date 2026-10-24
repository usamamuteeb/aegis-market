"""Actual Binance Spot execution using the persisted-intent broker and native OCO manager."""
import copy,time
from aegis.engine import Engine
from aegis.execution import Broker
from aegis.binance import Binance
from aegis.config import DEFAULTS
from aegis.domain import D,Blocked
from aegis.transport import RemoteError
from aegis.risk import plan,rollover
from aegis.intelligence.store import identity,encode

INITIAL={'position':None,'pending_entry':None,'mode':'PAUSED','generation':0,'seen':{},'daily_date':'',
 'daily_realized':'0','realized':'0','daily_entries':0,'consecutive_losses':0,'daily_halted':False,
 'last_exit_at':0,'peak_bot_equity':'10','usage':{'calls':0,'input_tokens':0,'output_tokens':0}}

class LiveLedger:
    def __init__(self,store,allocation):
        self.research=store;self.lock=store.lock;self.db=store.db
        self.state=store.state('live_state')
        if self.state is None:self.state={**copy.deepcopy(INITIAL),'peak_bot_equity':str(allocation)}
        self.state['mode']='PAUSED';self.state['generation']+=1
        self.settings={**DEFAULTS,'allocation':float(allocation),'symbols':['BTCUSDT','ETHUSDT','SOLUSDT','DOGEUSDT']}
        self.save(self.state)
    def save(self,state,event=None,intent=None,settings=None):
        with self.lock,self.db:
            self.research.put_state('live_state',state)
            if intent:
                intents=self.research.state('live_intents',{});intents[intent['id']]=intent;self.research.put_state('live_intents',intents)
            if event:self.event(event[0],**event[1])
        self.state=copy.deepcopy(state)
        if settings:self.settings=settings
    def event(self,kind,message='',**fields):
        self.research.append('observations',{'kind':kind,'message':message,**fields,'mode':'REAL'},version='live-event-v1')
    def intent(self,key):return self.research.state('live_intents',{}).get(key)
    def intents(self,statuses=None):
        return [v for v in self.research.state('live_intents',{}).values() if not statuses or v['status'] in statuses]
    def merge_trades(self,symbol,rows):
        with self.lock,self.db:
            for row in rows:
                rid=identity(['binance-fill',symbol,row['id']])
                if self.research.get('trade_episodes',rid):continue
                buy=row['isBuyer'];quantity=row['qty'];price=row['price'];at=row['time']/1000
                body={'kind':'ENTRY' if buy else 'EXIT','symbol':symbol,'wallet_key':'live:binance','mode':'REAL',
                      'filled_qty':quantity,'price':price,'position':{'entry':price},'reason':'Binance-confirmed fill',
                      'fee':row['commission'],'fee_asset':row['commissionAsset'],'exchange_order_id':row['orderId'],
                      'exchange_trade_id':row['id'],'fee_in_quote':row['commission'] if row['commissionAsset']=='USDT' else str(D(row['commission'])*D(price)) if row['commissionAsset']==symbol[:-4] else None}
                self.research.append('trade_episodes',body,symbol=symbol,as_of=at,version='binance-confirmed-fill-v1',record_id=rid)
    def record_session(self,closed):
        rows=self.research.state('live_closed',[])
        if not any(r['entry_intent']==closed['entry_intent'] for r in rows):rows.append(closed)
        self.research.put_state('live_closed',rows)

class LiveBridge(Engine):
    """Reuse reconciliation/protection, never the legacy scheduler or its model calls."""
    key='live:binance'
    def __init__(self,service,vault,allocation=10,enabled=False):
        self.service=service;self.vault=vault;self.api=Binance(vault)
        self.store=LiveLedger(service.store,allocation);self.broker=Broker(self.api,self.store)
        self.account=None;self.account_error=None;self.markets={};self.fees={};self.averages={}
        self.last_sync=0;self.last_messages={};self.last_poll=0;self.enabled=enabled;self.equity=service.store.state('live_equity',[])
        self.fault=None;self.last_ok=0
    def sync_account(self):
        account=self.api.request('account',omitZeroBalances='true')
        ident=identity([self.api.credentials()['key'],self.api.credentials()['environment']])
        bound=self.service.store.state('live_account_binding')
        if bound and bound!=ident:raise Blocked('This live ledger belongs to a different Binance key/account; use a separate database')
        if not bound:self.service.store.put_state('live_account_binding',ident)
        if account.get('accountType')!='SPOT':raise Blocked('A Binance Spot account is required')
        account['open_orders']=self.api.request('openOrders');account['synced_at']=time.time()
        self.account=account;self.last_sync=time.time()
        for symbol in self.store.settings['symbols']:
            if time.time()-self.fees.get(symbol,{}).get('at',0)>180:self.fees[symbol]=self.api.commission(symbol)
            if time.time()-self.averages.get(symbol,{}).get('at',0)>30:
                a=self.api.public('avgPrice',symbol=symbol)
                self.averages[symbol]={'price':a['price'],'mins':a['mins'],'at':time.time()}
        self.account_error=None
    def poll(self,force=False):
        if not self.api.status()['configured']:return
        if not force and time.monotonic()-self.last_poll<2:return
        self.last_poll=time.monotonic()
        try:
            if not self.account or time.time()-self.last_sync>10:self.sync_account()
            for symbol in self.store.settings['symbols']:
                try:
                    book=self.service.spot.book(symbol,self.service.market_now());book['received_at']=time.time()
                    a=self.averages.get(symbol)
                    if a:book.update(avg_price=a['price'],avg_mins=a['mins'])
                    self.markets[symbol]=book
                except (Blocked,KeyError):self.markets.pop(symbol,None)
            self.broker.reconcile();self._resolve_entry()
            p=self.state['position']
            if p and time.time()>=p['opened_at']+1800 and not p.get('exit_requested'):
                self.change(lambda s:s['position'].update(exit_requested='30-minute time exit'))
            self._risk_state();self._manage_position()
            metric=self.metrics()
            if metric['marked_equity'] is not None and (not self.equity or time.time()-self.equity[-1]['at']>=5):
                self.equity=(self.equity+[{'at':time.time(),'value':metric['marked_equity']}])[-10000:]
                self.service.store.put_state('live_equity',self.equity)
            self.last_ok=time.time();self.fault=None
        except Exception as exc:
            self.fault=('Binance rejected the API key, allowed IP or permissions (-2015)' if isinstance(exc,RemoteError) and exc.code==-2015 else str(exc)) if isinstance(exc,(Blocked,RemoteError)) else 'Binance account/order synchronization failed; retrying read-only reconciliation'
            self.notice('recovery',self.fault)
            if self.service.paper_mode=='RUNNING' and getattr(self.service,'execution_mode','virtual')=='real':self.service.pause()
    def arm(self,confirmation):
        if confirmation!='START REAL':raise Blocked('Type START REAL to enable Binance orders')
        if not self.enabled:raise Blocked('Set LIVE_TRADING_ENABLED=true in the worker environment before enabling real mode')
        if self.api.status()['environment']!='live':raise Blocked('Real mode requires Binance.com live credentials')
        self.poll(force=True)
        if self.fault or not self.account or time.time()-self.last_ok>15:raise Blocked(self.fault or 'Wait for account synchronization')
        if not self.account.get('canTrade'):raise Blocked('Binance API trading permission is unavailable')
        if self.broker.unresolved() or self.state.get('pending_entry'):raise Blocked('An ambiguous exchange request needs reconciliation')
        if self.account['open_orders'] and not self.state['position']:raise Blocked('Existing exchange orders need resolving before arming')
        free=next((D(b['free']) for b in self.account['balances'] if b['asset']=='USDT'),D(0))
        if free<D(self.settings['allocation']):raise Blocked('Available real USDT is below the configured allocation')
        self.change(lambda s:s.update(mode='RUNNING',generation=s['generation']+1))
    def preflight(self,candidate,book,now):
        if not self.enabled or self.state['mode']!='RUNNING':raise Blocked('Real trading is not armed')
        if self.fault or not self.account or time.time()-self.last_ok>15:raise Blocked('Real account reconciliation is stale')
        if self.state['position'] or self.state.get('pending_entry') or self.broker.unresolved():raise Blocked('A managed position or unresolved real order already exists')
        if candidate['id'] in self.state['seen']:raise Blocked('Duplicate real setup blocked')
        book=self.markets.get(candidate['symbol'])
        state=copy.deepcopy(self.state);rollover(state,time.time())
        local_candidate={**candidate,'expires_at':candidate['expires_at']-(now-time.time())}
        sized=plan(state,self.settings,self.account,local_candidate,book,self.fees.get(candidate['symbol']),time.time(),False)
        from aegis.domain import floor_step
        received=D(sized['qty'])*(1-D(self.fees[candidate['symbol']]['buy']))
        residual=(received-floor_step(received,book['rules']['step']))*D(book['bid'])
        residual+=sum((D(r['qty'])*D(self.markets.get(r['symbol'],{}).get('bid',r.get('mark_price','0'))) for r in self.state.get('residuals',[])),D(0))
        if residual>D('.10'):raise Blocked('Managed residual assets exceed the 0.10 USDT exposure cap; review dust before another entry')
        return sized
    def enter(self,candidate,book,now,expected_generation=None):
        with self.service.lock,self.store.lock:
            if expected_generation is not None and (expected_generation!=self.service.generation or self.service.paper_mode!='RUNNING'):raise Blocked('Real submission invalidated by Pause or recovery')
            sized=self.preflight(candidate,book,now)
            if now>=candidate['expires_at']:raise Blocked('Proposal expired before real submission')
            base=next((b for b in self.account['balances'] if b['asset']==candidate['symbol'][:-4]),{'free':'0','locked':'0'})
            pending={'key':'entry:'+candidate['id'],'candidate':copy.deepcopy(candidate),'base_before':str(D(base['free'])+D(base['locked']))}
            self.change(lambda s:(s.update(pending_entry=pending),s['seen'].update({candidate['id']:now})))
            live_generation=self.state['generation']
        intent=self.broker.entry(candidate,sized,live_generation)
        self._resolve_entry();self._manage_position()
        return {'accepted':intent['status']=='ACCEPTED' and self.state['position'] is not None,
                'reason':'Binance order '+intent['status']+'; see actual fills and protection status'}
    def view_state(self):
        p=self.state['position'];mapped=None
        if p:mapped={**p,'entry':p['entry_price'],'qty':p['remaining_qty'],'deadline':p['opened_at']+1800,'protection_state':p.get('protection_verified_state',p['status'])}
        free=next((b['free'] for b in (self.account or {}).get('balances',[]) if b['asset']=='USDT'),None)
        return {'position':mapped,'cash':free,'initial':str(self.settings['allocation']),'equity':self.equity}
    def metrics(self):
        closed=self.service.store.state('live_closed',[]);values=[D(x['net_pnl']) for x in closed if x['net_pnl'] is not None]
        wins=[x for x in values if x>0];losses=[x for x in values if x<0];view=self.view_state()
        fees=[r['body'] for r in self.service.store.records('trade_episodes',limit=10000) if r['body'].get('wallet_key')==self.key]
        unknown=any(r.get('fee_in_quote') is None for r in fees)
        equity=D(self.settings['allocation'])+D(self.state['realized']);p=self.state['position']
        if p and p['symbol'] in self.markets:equity+=D(p['remaining_qty'])*D(self.markets[p['symbol']]['bid'])+D(p.get('exit_proceeds',0))-D(p['cost'])
        for residual in self.state.get('residuals',[]):equity+=D(residual['qty'])*D(self.markets.get(residual['symbol'],{}).get('bid',residual.get('mark_price','0')))-D(residual.get('cost','0'))
        exact=bool(self.account) and not unknown and not self.state.get('pnl_unvalued')
        return {'open_position':view['position'],'marked_equity':str(equity) if exact else None,'net_total':str(equity-D(self.settings['allocation'])) if exact else None,
         'net_realized':None if self.state.get('pnl_unvalued') else self.state['realized'],'closed_trades':len(closed),
         'win_rate':len(wins)/len(values) if values and len(values)==len(closed) else None,
         'average_win':str(sum(wins)/len(wins)) if wins else None,'average_loss':str(sum(losses)/len(losses)) if losses else None,
         'profit_factor':str(sum(wins)/-sum(losses)) if losses else None,
         'fees':None if unknown else str(sum((D(r['fee_in_quote']) for r in fees),D(0))),
         'max_drawdown':str(D(self.state.get('max_bot_drawdown',0))/D(self.state['peak_bot_equity'])) if D(self.state['peak_bot_equity'])>0 else None,
         'cash':view['cash'],'allocation':str(self.settings['allocation']),'account_synced_at':self.last_sync,
         'assets':[{k:b[k] for k in ('asset','free','locked')} for b in (self.account or {}).get('balances',[])],
         'unresolved_orders':len(self.broker.unresolved()),'pending_entry':bool(self.state.get('pending_entry')),'residuals':self.state.get('residuals',[]),'error':self.fault,'enabled':self.enabled,
         'connected':bool(self.account),'intents':self.store.intents()[:50],
         'risk_policy':'Actual fills and native exchange OCO. Entry-to-OCO gap and gaps through stops can exceed planned loss.'}
