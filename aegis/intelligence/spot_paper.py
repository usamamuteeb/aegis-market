"""Received-asset commissions and explicit dust; isolated virtual accounting only."""
import copy
import time
from decimal import Decimal,ROUND_CEILING
from .paper import PaperBroker
from aegis.domain import D,Blocked,floor_step
from aegis.risk import plan,filters,protective_filters,rollover

VERSION='received-asset-paper-v1'


class SpotPaper(PaperBroker):
    def __init__(self,store,key='spot-lab-v1',balance=10,*,dust_stress=False,**kw):
        super().__init__(store,key,balance,**kw)
        self.state.setdefault('dust',{});self.state.setdefault('accounting_version',VERSION)
        self.bids=dict(self.state.get('dust_bids',{}));self.settings['slippage_bps']=float(self.slip*10000)
        self.dust_stress=dust_stress;self.dust_cap=D('.10')
        if self.state['accounting_version']!=VERSION:raise ValueError('Incompatible paper ledger')
        self._commit()

    def _amounts(self,qty,book,candidate):
        price=D(book['ask'])*(1+self.slip)
        unit=D(10)**-int(book['rules'].get('base_commission_precision',8))
        fee=(qty*self.fee).quantize(unit,rounding=ROUND_CEILING)
        previous=self.state['dust'].get(candidate['symbol'],{'qty':'0','cost':'0'})
        received=qty-fee+D(previous['qty']);sellable=floor_step(received,book['rules']['step']);dust=received-sellable
        if received<=0 or sellable<=0:raise Blocked('Commission leaves no sellable quantity')
        cost=qty*price;basis=cost+D(previous['cost'])
        stop_net=sellable*D(candidate['stop_loss'])*(1-self.slip)*(1-self.fee)
        target_net=sellable*D(candidate['take_profit'])*(1-self.slip)*(1-self.fee)
        residual_stop=dust*D(candidate['stop_loss'])*(1-self.slip)*(1-self.fee)
        residual_target=dust*D(candidate['take_profit'])*(1-self.slip)*(1-self.fee)
        risk=basis-stop_net-(D(0) if self.dust_stress else residual_stop)
        reward=target_net-basis+(D(0) if self.dust_stress else residual_target)
        # Normal policy measures planned marked loss at the stop, and separately
        # caps total residual exposure. Zero-recovery is an explicit stress arm.
        return {'gross_qty':qty,'received':received,'sellable':sellable,'dust':dust,'base_fee':fee,
                'entry':price,'cost':cost,'basis':basis,'risk':risk,'reward':reward,
                'risk_if_residual_zero':basis-stop_net,'residual_value':dust*price}

    def preflight(self,candidate,book,now):
        from .features import POLICY
        policy=POLICY[candidate['interval']]
        if not 0<=now-book['received_at']<=policy['max_feed_age']:raise Blocked('Market quote stale')
        if now>=candidate['expires_at']:raise Blocked('Setup expired')
        if abs(D(book['ask'])/D(candidate['entry'])-1)*10000>D(policy['max_deviation_bps']):raise Blocked('Price moved beyond entry tolerance')
        # plan reads nested values; rollover only replaces top-level day counters.
        # Copying the complete equity/trade history here would stall growing wallets.
        state=dict(self.state);rollover(state,now)
        account={'synced_at':now,'canTrade':True,'open_orders':[],
                 'balances':[{'asset':'USDT','free':state['cash'],'locked':'0'}]}
        # No quote-currency BUY charge: entry commission is withheld from base.
        upper=plan(state,self.settings,account,candidate,book,{'buy':'0','sell':str(self.fee),'at':now},now,False)
        q=D(upper['qty']);step=D(book['rules']['step']);budget=min(D(self.settings['risk_per_trade']),D(self.settings['daily_loss_limit'])+D(state['daily_realized']))
        attempts=0
        while q>0 and attempts<10000:
            if q<D(book['rules']['min_qty']) or q*D(book['ask'])<D(book['rules']['min_notional']):break
            a=self._amounts(q,book,candidate)
            other_dust=sum((D(d['qty'])*D(self.bids.get(s,0)) for s,d in self.state['dust'].items() if s!=candidate['symbol']),D(0))
            if D(0)<a['risk']<=budget and a['reward']>=a['risk'] and other_dust+a['residual_value']<=self.dust_cap:
                filters(q,book,D(candidate['stop_loss'])*(1-self.slip)*(1-self.fee))
                filters(a['sellable'],book,D(candidate['stop_loss'])*(1-self.slip)*(1-self.fee))
                protective_filters(a['sellable'],candidate['stop_loss'],candidate['take_profit'],book)
                return {**{k:str(v) for k,v in a.items()},'qty':str(q),'fee_basis':'Assumed 0.10% per side; not an authenticated account fee'}
            q-=step;attempts+=1
        raise Blocked('No quantity fits minimums, fees, the $0.10 residual cap and $0.10 planned-risk budget')

    def enter(self,candidate,book,now,*,fill_fraction=1):
        started=time.monotonic()
        with self.store.lock:
            self.state=self.store.state(self.key,self.state);rollover(self.state,now)
            if candidate['id'] in self.state['seen']:return {'accepted':False,'reason':'Duplicate setup blocked'}
            try:
                if not 0<D(fill_fraction)<=1:raise Blocked('Invalid paper fill fraction')
                sized=self.preflight(candidate,book,now)
                qty=floor_step(D(sized['qty'])*D(fill_fraction),book['rules']['step'])
                a=self._amounts(qty,book,candidate)
                # Filled residual must remain protectable. This is a simulation,
                # not a claim that a real exchange can undo a partial fill.
                filters(a['sellable'],book,D(candidate['stop_loss'])*(1-self.slip)*(1-self.fee))
                protective_filters(a['sellable'],candidate['stop_loss'],candidate['take_profit'],book)
                if a['cost']>D(self.state['cash']):raise Blocked('Insufficient quote balance')
                if a['risk']>D(self.settings['risk_per_trade']) or a['reward']<a['risk']:raise Blocked('Partial fill violates fee-aware risk')
                self.state['cash']=str(D(self.state['cash'])-a['cost'])
                self.state['costs']=str(D(self.state['costs'])+a['base_fee']*a['entry'])
                self.state['turnover']=str(D(self.state.get('turnover','0'))+a['cost'])
                usable_cost=a['basis']*a['sellable']/a['received'];dust_cost=a['basis']-usable_cost
                self.state['dust'][candidate['symbol']]={'qty':str(a['dust']),'cost':str(dust_cost)}
                self.state['position']={'id':'paper:'+candidate['id'],'symbol':candidate['symbol'],'candidate_id':candidate['id'],
                    'strategy':candidate.get('strategy_version'),'qty':str(a['sellable']),'original_qty':str(a['sellable']),
                    'entry':str(a['entry']),'cost':str(usable_cost),'original_cost':str(usable_cost),
                    'stop_loss':candidate['stop_loss'],'take_profit':candidate['take_profit'],'opened_at':now,
                    'deadline':now+candidate.get('holding_minutes',30)*60,'interval':'1m','exit_proceeds':'0',
                    'rules':book['rules'],'protection_state':'LOCAL_PAPER_RULES_ONLY','risk_reserved':str(a['risk']),
                    'risk_if_residual_zero':str(a['risk_if_residual_zero']),
                    'partial_entry':qty<D(sized['qty']),'unfilled_remainder':'Not submitted; simulation only'}
                self.state['daily_entries']+=1;self.state['seen']=(self.state['seen']+[candidate['id']])[-10000:]
                self.bids[candidate['symbol']]=str(book['bid'])
                self._commit({'kind':'ENTRY','symbol':candidate['symbol'],'position':copy.deepcopy(self.state['position']),
                    'commission':str(a['base_fee']*a['entry']),'commission_asset':candidate['symbol'][:-4],
                    'commission_quantity':str(a['base_fee']),'requested_qty':sized['qty'],'filled_qty':str(qty),
                    'sellable_qty':str(a['sellable']),'dust_qty':str(a['dust']),'accounting_version':VERSION},now)
                self.mark(book['bid'],now);self._timing('PAPER_ENTRY',candidate,book,now,started)
                return {'accepted':True,'position':self.state['position']}
            except Blocked as exc:return self.skip(str(exc),now)

    def observe(self,symbol,bid,now):
        self.bids[symbol]=str(bid)
        p=self.state['position']
        if p and p['symbol']==symbol:return self.monitor(bid,now)
        if not self.state['equity'] or now-self.state['equity'][-1]['at']>=5:
            self.mark(self.bids.get(p['symbol'],p['entry']) if p else bid,now)

    def mark(self,bid,now):
        p=self.state['position'];liquid=D(self.state['cash'])
        if p:
            self.bids[p['symbol']]=str(bid)
            liquid+=D(p['qty'])*D(bid)*(1-self.slip)*(1-self.fee)
        dust_value=sum((D(d['qty'])*D(self.bids.get(s,0)) for s,d in self.state.get('dust',{}).items()),D(0))
        equity=liquid+dust_value
        self.state['liquidatable_equity']=str(liquid);self.state['dust_marked_value']=str(dust_value)
        self.state['dust_bids']=dict(self.bids)
        self.state['peak_bot_equity']=str(max(D(self.state['peak_bot_equity']),equity))
        drawdown=(D(self.state['peak_bot_equity'])-equity)/D(self.state['peak_bot_equity'])
        self.state['max_drawdown_marked']=str(max(D(self.state.get('max_drawdown_marked','0')),drawdown))
        if drawdown>=D(self.settings['max_drawdown_pct'])/100:self.state['daily_halted']=True
        point={'at':now,'value':str(equity),'open_position':bool(p)}
        if self.state['equity'] and int(self.state['equity'][-1]['at']//5)==int(now//5):self.state['equity'][-1]=point
        else:self.state['equity'].append(point)
        self.state['equity']=self.state['equity'][-10000:];self._commit(now=now);return equity

    def metrics(self):
        return {**super().metrics(),'accounting_version':VERSION,'fee_asset':'BASE_ON_BUY_QUOTE_ON_SELL',
            'dust':copy.deepcopy(self.state['dust']),'dust_marked_value':self.state.get('dust_marked_value','0'),
            'liquidatable_equity':self.state.get('liquidatable_equity',self.state['cash']),
            'dust_cap_USDT':str(self.dust_cap),'risk_policy':'Planned marked stop loss; residual exposure capped separately at 0.10 USDT. Not a guaranteed loss ceiling.',
            'dust_note':'Residual assets are marked at the last observed bid, not available cash; cannot assume immediate sale.'}
