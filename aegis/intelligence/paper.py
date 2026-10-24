"""Decimal paper accounting and independent stop management, with durable events."""
import copy
import time
from decimal import Decimal
from .store import encode,identity
from .features import POLICY
from aegis.domain import D,Blocked,floor_step
from aegis.config import DEFAULTS
from aegis.risk import plan,rollover,filters

def reference_book(symbol,price,rules,now,spread_bps=10):
    price=D(price);half=D(spread_bps)/20000
    return {'symbol':symbol,'bid':str(price*(1-half)),'ask':str(price*(1+half)),'mid':str(price),
            'spread_bps':spread_bps,'rules':rules,'received_at':now,'avg_mins':5,'avg_price':str(price),
            'source':'Declared historical quote proxy; not reconstructed depth'}

class PaperBroker:
    def __init__(self,store,key,balance=10,*,fee='.001',slippage_bps=10):
        if D(balance) not in (D(5),D(10)):raise ValueError('First paper workflow supports explicit $5/$10 balances')
        self.store=store;self.key='paper:'+key;self.fee=D(fee);self.slip=D(slippage_bps)/10000
        existing=store.state(self.key)
        self.state=existing or {'initial':str(balance),'cash':str(balance),'position':None,'mode':'RUNNING',
            'seen':[],'daily_date':'','daily_realized':'0','realized':'0','daily_entries':0,'consecutive_losses':0,
            'daily_halted':False,'last_exit_at':0,'peak_bot_equity':str(balance),'closed':[],'equity':[],
            'costs':'0','turnover':'0','max_drawdown_marked':'0','skip_reasons':{},'label':'PAPER ONLY','last_bar':None}
        self.settings={**DEFAULTS,'allocation':float(balance)}
        if not existing:store.put_state(self.key,self.state)

    def _commit(self,event=None,now=None):
        with self.store.lock,self.store.db:
            self.store.db.execute('INSERT INTO state VALUES(?,?) ON CONFLICT(key) DO UPDATE SET body=excluded.body',(self.key,encode(self.state)))
            if event:
                rid=identity([self.key,event]);self.store.db.execute('INSERT OR IGNORE INTO trade_episodes VALUES(?,?,?,?,?,?,?)',
                    (rid,event['symbol'],now,now,now,'decimal-paper-v1',encode({**event,'wallet_key':self.key,'mode':'PAPER'})))

    def skip(self,reason,now):
        self.state['skip_reasons'][reason]=self.state['skip_reasons'].get(reason,0)+1;self._commit(now=now)
        return {'accepted':False,'reason':reason}

    def preflight(self,candidate,book,now):
        """Pure local feasibility screen, before spending a model request."""
        policy=POLICY[candidate['interval']]
        if not 0<=now-book['received_at']<=policy['max_feed_age']:raise Blocked('Timeframe feed is stale')
        if now>=candidate['expires_at']:raise Blocked('Candidate expired before dispatch')
        if abs(D(book['ask'])/D(candidate['entry'])-1)*10000>D(policy['max_deviation_bps']):raise Blocked('Timeframe entry deviation exceeded')
        account={'synced_at':now,'canTrade':True,'open_orders':[],'balances':[{'asset':'USDT','free':self.state['cash'],'locked':'0'}]}
        state=copy.deepcopy(self.state);rollover(state,now)
        return plan(state,self.settings,account,candidate,book,{'buy':str(self.fee),'sell':str(self.fee),'at':now},now,False)

    def enter(self,candidate,book,now,*,fill_fraction=1):
        dispatch_started=time.monotonic()
        with self.store.lock:
            self.state=self.store.state(self.key,self.state)
            if candidate['id'] in self.state['seen']:return {'accepted':False,'reason':'Candidate already processed; duplicate paper entry blocked'}
            rollover(self.state,now)
            if not 0<D(fill_fraction)<=1:return self.skip('No available paper liquidity',now)
            try:
                sized=self.preflight(candidate,book,now)
                qty=floor_step(D(sized['qty'])*D(fill_fraction),book['rules']['step'])
                if qty<=0:raise Blocked('Partial fill rounds to zero quantity')
                price=D(book['ask'])*(1+self.slip);cost=qty*price;commission=cost*self.fee
                if cost+commission>D(self.state['cash']):raise Blocked('Paper cash insufficient including entry fee')
                if not D(candidate['stop_loss'])<price<D(candidate['take_profit']):raise Blocked('Entry no longer lies inside approved barriers')
                self.state['cash']=str(D(self.state['cash'])-cost-commission)
                self.state['costs']=str(D(self.state['costs'])+commission)
                self.state['turnover']=str(D(self.state.get('turnover','0'))+cost)
                self.state['position']={'id':'paper:'+candidate['id'],'symbol':candidate['symbol'],'candidate_id':candidate['id'],
                    'qty':str(qty),'entry':str(price),'cost':str(cost+commission),'original_cost':str(cost+commission),
                    'original_qty':str(qty),'stop_loss':candidate['stop_loss'],'take_profit':candidate['take_profit'],
                    'opened_at':now,'deadline':now+POLICY[candidate['interval']]['horizon']*(60 if candidate['interval']=='1m' else 300),
                    'interval':candidate['interval'],'exit_proceeds':'0','rules':book['rules'],
                    'protection_state':'LOCAL_PAPER_RULES_ONLY','partial_entry':qty<D(sized['qty']),
                    'unfilled_remainder':'Cancelled in the paper simulation; no exchange submission'}
                self.state['daily_entries']+=1;self.state['seen'].append(candidate['id'])
                self._commit({'kind':'ENTRY','symbol':candidate['symbol'],'position':copy.deepcopy(self.state['position']),
                              'commission':str(commission),'requested_qty':sized['qty'],'filled_qty':str(qty)},now)
                self.mark(book['bid'],now)
                self._timing('PAPER_ENTRY',candidate,book,now,dispatch_started)
                return {'accepted':True,'position':self.state['position']}
            except Blocked as exc:return self.skip(str(exc),now)

    def _timing(self,kind,candidate,book,now,started):
        elapsed=time.monotonic()-started
        self.store.append('latencies',{'event_type':kind,'processing_ms':elapsed*1000,
            'source_event_at':book.get('source_event_at'),'received_at':book.get('received_at'),
            'completed_candle_at':candidate.get('data_cutoff'),'candidate_at':candidate.get('created_at'),
            'assessment_at':candidate.get('assessment_at'),'dispatch_at':now,'ack_at':now+elapsed,'fill_at':now+elapsed,
            'clock_offset_uncertainty_ms':None,'network_latency_ms':None,
            'time_basis':'Paper logical event time plus measured monotonic processing; not exchange acknowledgements/fills'},
            symbol=candidate['symbol'],as_of=now)

    def mark(self,bid,now):
        p=self.state['position'];equity=D(self.state['cash'])
        if p:equity+=D(p['qty'])*D(bid)*(1-self.slip)*(1-self.fee)
        self.state['peak_bot_equity']=str(max(D(self.state['peak_bot_equity']),equity))
        drawdown=(D(self.state['peak_bot_equity'])-equity)/D(self.state['peak_bot_equity'])
        self.state['max_drawdown_marked']=str(max(D(self.state.get('max_drawdown_marked','0')),drawdown))
        if equity<=D(self.state['peak_bot_equity'])*(1-D(self.settings['max_drawdown_pct'])/100):self.state['daily_halted']=True
        point={'at':now,'value':str(equity),'open_position':bool(p)}
        if self.state['equity'] and int(self.state['equity'][-1]['at']//5)==int(now//5):self.state['equity'][-1]=point
        else:self.state['equity'].append(point)
        self.state['equity']=self.state['equity'][-10000:]
        self._commit(now=now);return equity

    def monitor(self,bid,now,*,high=None,low=None,opening=None,fill_fraction=1):
        """Tick exits do not need a candle close or successful model call."""
        dispatch_started=time.monotonic()
        with self.store.lock:
            rollover(self.state,now)
            p=self.state['position'];reason=None;price=D(bid)
            if p:
                stop,target=D(p['stop_loss']),D(p['take_profit'])
                if D(low if low is not None else bid)<=stop:
                    reason='STOP';price=min(D(opening if opening is not None else bid),stop)
                elif D(high if high is not None else bid)>=target:reason='TARGET';price=target
                elif now>=p['deadline']:reason='TIMEOUT'
                if reason:
                    qty=floor_step(D(p['qty'])*D(fill_fraction),p['rules']['step'])
                    book=reference_book(p['symbol'],price,p['rules'],now,0)
                    try:filters(qty,book,price)
                    except Blocked as exc:
                        changed=p.get('exit_blocked')!=str(exc)
                        p['exit_blocked']=str(exc);p['protection_state']='PAPER_EXIT_BLOCKED_DUST_OR_FILTER'
                        if changed or not self.state['equity'] or now-self.state['equity'][-1]['at']>=5:self.mark(bid,now)
                        return {'closed':False,'reason':str(exc)}
                    execution=price*(1-self.slip);gross=qty*execution;fee=gross*self.fee;proceeds=gross-fee
                    allocated=D(p['cost'])*qty/D(p['qty']);realized=proceeds-allocated
                    self.state['cash']=str(D(self.state['cash'])+proceeds);self.state['costs']=str(D(self.state['costs'])+fee)
                    self.state['turnover']=str(D(self.state.get('turnover','0'))+gross)
                    self.state['realized']=str(D(self.state['realized'])+realized);self.state['daily_realized']=str(D(self.state['daily_realized'])+realized)
                    p['qty']=str(D(p['qty'])-qty);p['cost']=str(D(p['cost'])-allocated);p['exit_proceeds']=str(D(p['exit_proceeds'])+proceeds)
                    event={'kind':'EXIT','symbol':p['symbol'],'position_id':p['id'],'reason':reason,'filled_qty':str(qty),
                           'price':str(execution),'fee':str(fee),'net_pnl':str(realized),'remaining_qty':p['qty']}
                    if D(p['qty'])==0:
                        net=D(p['exit_proceeds'])-D(p['original_cost'])
                        self.state['closed'].append({'id':p['id'],'opened_at':p['opened_at'],'closed_at':now,'net_pnl':str(net),'reason':reason})
                        self.state['consecutive_losses']=self.state['consecutive_losses']+1 if net<0 else 0
                        self.state['last_exit_at']=now;self.state['position']=None
                    self._commit(event,now)
                    self._timing('PAPER_EXIT',{'symbol':event['symbol']},{'received_at':now},now,dispatch_started)
            if reason or not self.state['equity'] or now-self.state['equity'][-1]['at']>=5:self.mark(bid,now)
            return {'closed':bool(reason and not self.state['position']),'reason':reason}

    def metrics(self):
        s=self.state;values=[D(x['net_pnl']) for x in s['closed']];wins=[x for x in values if x>0];losses=[x for x in values if x<0]
        equity=D(s['equity'][-1]['value']) if s['equity'] else D(s['initial']);peak=D(s['initial']);dd=D(0)
        for row in s['equity']:
            value=D(row['value']);peak=max(peak,value);dd=max(dd,(peak-value)/peak)
        n=len(values);n_win=len(wins)
        return {'closed_trades':n,'net_realized':s['realized'],'marked_equity':str(equity),'marked_at':s['equity'][-1]['at'] if s['equity'] else None,'net_total':str(equity-D(s['initial'])),
                'open_position':s['position'],'fees':s['costs'],'win_rate':n_win/n if n else None,
                'average_win':str(sum(wins)/len(wins)) if wins else None,'average_loss':str(sum(losses)/len(losses)) if losses else None,
                'profit_factor':str(sum(wins)/-sum(losses)) if losses else None,'max_drawdown':str(max(dd,D(s.get('max_drawdown_marked','0')))),
                'exposure_fraction':sum(x['open_position'] for x in s['equity'])/len(s['equity']) if s['equity'] else None,
                'skip_reasons':s['skip_reasons'],'turnover':s.get('turnover','0'),'claim':'Paper results include open losing exposure; no live release approval'}
