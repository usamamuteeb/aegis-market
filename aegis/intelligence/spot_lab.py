"""Prospective GPT-filtered Spot paper policy plus a separate no-GPT comparator."""
import copy
import hashlib
import json
import time
from pathlib import Path
from .spot_strategy import PAIRS,STRATEGIES,SPEC,propose
from .spot_paper import SpotPaper
from .spot_model import SpotAnalyst
from .paper import reference_book
from .store import identity
from aegis.market import parse_rules
from aegis.domain import Blocked


class SpotLab:
    def __init__(self,service):
        self.service=service;self.store=service.store
        self.paper=SpotPaper(self.store,'spot-gpt-v1',10);self.baseline=SpotPaper(self.store,'spot-baseline-v1',10)
        self.analyst=SpotAnalyst(self.store,service.market_now);self.future=None;self.seen={};self.scans={};self.last_tick=0;self.last_timing=0
        self.spec_hash=identity(SPEC)
        for broker in (self.paper,self.baseline):
            previous=broker.state.get('spec_hash')
            if previous and previous!=self.spec_hash:raise ValueError('Strategy specification changed; use a separate paper ledger')
            broker.state['spec_hash']=self.spec_hash;broker._commit()
        p=Path(__file__).resolve().parents[2]/'spot-study-summary.json'
        self.study=json.loads(p.read_text(encoding='utf-8')) if p.exists() else None
        here=Path(__file__).resolve().parent
        expected=hashlib.sha256((here/'spot_strategy.py').read_bytes()+(here/'spot_paper.py').read_bytes()).hexdigest()
        if self.study and self.study.get('source_sha256')!=expected:self.study=None

    def prepare_markets(self,cancel):
        s=self.service;fresh={}
        for symbol in PAIRS:
            if cancel.is_set() or s.stop.is_set():return
            s.job['message']='Checking Spot filters: '+symbol
            product=s.ingestor.client.get('exchangeInfo',symbol=symbol)['symbols'][0]
            if product['symbol']!=symbol or product['quoteAsset']!='USDT':raise ValueError('Unexpected public market identity')
            rules=parse_rules(product);rules['base_commission_precision']=product.get('baseCommissionPrecision',8)
            for interval in ('1m','5m'):
                old=s.prepared.get((symbol,interval),{})
                fresh[(symbol,interval)]={'rows':old.get('rows',[]),'rules':rules,'dataset_id':'spot-warmup:'+symbol+':'+interval,
                     'dataset':{'symbol':symbol,'interval':interval,'coverage':'Recent public warmup only'}}
        if not cancel.is_set():s.prepared=fresh

    def restore(self):
        for key,p in self.store.state('spot_recent_frames',{}).items():
            symbol,interval=key.split(':')
            if symbol in PAIRS and interval in ('1m','5m'):self.service.prepared[(symbol,interval)]=p

    def save_frames(self):
        self.store.put_state('spot_recent_frames',{s+':'+i:p for (s,i),p in self.service.prepared.items()})

    def resume_exits(self,stream_factory=None):
        """Restore monitoring for held virtual positions without rearming entries."""
        if not (self.paper.state['position'] or self.baseline.state['position']):return False
        from .stream import PublicStream
        s=self.service;s.pause()
        if not s.prepared:self.prepare_markets(s.stop)
        if not getattr(s,'stream',None):
            s.stream=(stream_factory or PublicStream)(s,sorted({symbol for symbol,_ in s.prepared}),sorted({i for _,i in s.prepared}))
        s.stream.start();return True

    def observe(self,symbol,bid,now):
        # Called on the public quote thread: no model work or strategy scan here.
        started=time.monotonic()
        self.paper.observe(symbol,bid,now);self.baseline.observe(symbol,bid,now)
        if started-self.last_timing>=30:
            self.last_timing=started
            self.store.append('latencies',{'event_type':'SPOT_PAPER_QUOTE','processing_ms':(time.monotonic()-started)*1000,
                'clock_uncertainty_seconds':self.service.market_clock.status().get('uncertainty_seconds'),
                'limitation':'Local paper quote/exit processing only; not exchange execution latency'},symbol=symbol,as_of=now,version='spot-latency-v1')

    def active(self):return self.paper

    def active_state(self):
        broker=self.active();return broker.view_state() if hasattr(broker,'view_state') else broker.state

    def ready(self):
        s=self.service;stream=getattr(s,'stream',None)
        return s.paper_mode=='RUNNING' and s.market_clock.status()['ready'] and bool(stream and stream.ready.is_set())

    def book(self,symbol,now):
        s=self.service;q=s.last_feed.get(symbol)
        if not q or not 0<=time.time()-q['received_at']<=3:raise Blocked('Waiting for a fresh public quote')
        rules=s.prepared[(symbol,'1m')]['rules'];book=reference_book(symbol,(q['bid']+q['ask'])/2,rules,now,0)
        book.update(bid=str(q['bid']),ask=str(q['ask']),spread_bps=(q['ask']-q['bid'])/((q['bid']+q['ask'])/2)*10000)
        return book

    def record(self,symbol,action,reason,now,**extra):
        self.store.append('decisions',{'action':action,'reason':reason,'source':'spot-lab-forward',
                          'wallet_key':self.active().key,'mode':getattr(self.service,'execution_mode','virtual').upper(),**extra},symbol=symbol,as_of=now)

    def packet(self,c,book,sized,one,five,now):
        cid=c['id'];eid='setup:'+cid
        return {'version':'spot-evidence-v1','as_of':now,'candidate':copy.deepcopy(c),'evidence_ids':[eid],
            'strategy_spec':SPEC,'indicators':c['indicators'],
            'completed_1m_candles':one[-8:],'completed_5m_candles':five[-6:],
            'market':{k:book[k] for k in ('bid','ask','spread_bps')},
            'wallet':{'virtual':getattr(self.service,'execution_mode','virtual')!='real','cash_USDT':self.active_state()['cash'],'position':None},
            'coded_order_check':sized,'evidence':{'id':eid,'type':'completed-public-candles-and-local-feasibility'},
            'historical_status':{'new_hypotheses':'Unvalidated','study_status':self.study.get('status') if self.study else 'No completed study',
                 'warning':'No probability of profit has been established. Do not invent a win rate or statistical evidence.'},
            'instructions':'Assess the fixed setup only. BUY approves existing immutable levels; WAIT rejects. Never change size, levels, strategy or coded limits.'}

    def tick(self):
        s=self.service;now=s.market_now()
        if self.future and self.future.done():
            finished=self.future;self.future=None
            packet,c,generation,result=finished.result()
            reason=result['reason'];accepted=False
            if result['decision']=='BUY':
                try:
                    with s.lock:
                        if generation!=s.generation or not self.ready():raise Blocked('Approval invalidated by Pause or connection recovery')
                        if now>=min(c['expires_at'],result['valid_until']):raise Blocked('Approval expired before execution')
                        if s.prepared[(c['symbol'],'1m')]['rows'][-1]['close_ms']/1000!=c['data_cutoff']:raise Blocked('New candle invalidated approval')
                        broker=self.active();book=self.book(c['symbol'],now)
                        if not hasattr(broker,'view_state'):out=broker.enter(c,book,now)
                    if hasattr(broker,'view_state'):out=broker.enter(c,book,now,expected_generation=generation)
                    accepted=out['accepted'];reason=out.get('reason',reason)
                except (Blocked,KeyError) as exc:reason=str(exc)
            self.record(c['symbol'],'BUY' if accepted else 'WAIT',reason,now,candidate_id=c['id'],
                        strategy=c['strategy_version'],assessment_id=result.get('assessment_id'))
            self.scans[c['symbol']]={'at':now,'stage':'BUY FILLED' if accepted else 'WAIT','reason':reason,'strategy':c['strategy_version']}
        if not self.ready() or now-self.last_tick<.5:return
        self.last_tick=now;primary_selected=bool(self.future);baseline_selected=False
        for symbol in PAIRS:
            one=s.prepared.get((symbol,'1m'),{}).get('rows',[]);five=s.prepared.get((symbol,'5m'),{}).get('rows',[])
            if not one or not five:continue
            frame=(one[-1]['close_ms'],five[-1]['close_ms'])
            if self.seen.get(symbol)==frame:continue
            try:
                book=self.book(symbol,now);candidates=propose(symbol,one,five,now,book['ask'],book['rules']['tick'])
            except (ValueError,Blocked) as exc:
                self.scans[symbol]={'at':now,'stage':'DATA','reason':str(exc)};continue
            self.seen[symbol]=frame
            reasons=[]
            for c in candidates:
                if not c['eligible']:reasons.append(c['strategy_version']+': '+c['reason']);continue
                if not baseline_selected and getattr(s,'execution_mode','virtual')=='virtual':
                    with s.lock:
                        if not self.ready():return
                        result=self.baseline.enter(c,book,now)
                    if result['accepted']:baseline_selected=True
                try:
                    sized=self.active().preflight(c,book,now)
                    if primary_selected:raise Blocked('One model assessment is already in progress')
                    if not self.analyst.economy.enabled:raise Blocked('Connect GPT to assess eligible entries')
                    usage=self.analyst.economy.status()
                    if usage['calls_rolling_hour']>=usage['hourly_ceiling'] or usage['calls_rolling_24h']>=usage['daily_ceiling']:raise Blocked('GPT call budget reached; waiting for the rolling allowance to reset')
                    packet=self.packet(c,book,sized,one,five,now);generation=s.generation
                    self.future=s.assessment_pool.submit(lambda packet=packet,c=c,g=generation:(packet,c,g,self.analyst.assess(packet)))
                    primary_selected=True
                    reasons.append('Eligible setup; GPT assessment queued')
                    self.record(symbol,'ASSESS',reasons[-1],now,candidate_id=c['id'],strategy=c['strategy_version'])
                except Blocked as exc:reasons.append(c['strategy_version']+': '+str(exc))
            message='; '.join(reasons)
            self.scans[symbol]={'at':now,'stage':'GPT' if 'queued' in message else 'WAIT','reason':message}
            self.record(symbol,'WAIT',message,now,data_cutoff=one[-1]['close_ms']/1000)

    def snapshot(self):
        return {'version':'spot-lab-v1','spec':SPEC,'paper_key':self.paper.key,'baseline_key':self.baseline.key,
            'scans':copy.deepcopy(self.scans),'assessment_pending':bool(self.future),
            'model_usage':self.analyst.economy.status(),'model':self.analyst.model,'clock':self.service.market_clock.status(),
            'baseline':self.baseline.metrics(),'baseline_note':'Independent virtual no-GPT comparator on the same signal stream; wallet paths and entry times can differ. Not real funds or a controlled causal trial.',
            'study':self.study,'model_improvement':'Unmeasured; historical tests contain no GPT calls. Forward comparison needs sufficient data.',
            'live_eligible':False}
