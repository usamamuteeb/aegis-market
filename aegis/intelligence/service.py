"""Public-data research orchestration. It cannot instantiate an exchange account."""
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from .store import TABLES,identity,source_fingerprint
from .data import Ingestor,INTERVALS,numeric,PublicDataError
from .features import features,candidate,profile,POLICY
from .forecast import Predictor,resolve
from .evaluation import evaluate_dataset,examples,chronological
from .memory import retrieve,remember
from .model import EvidenceAnalyst
from .paper import PaperBroker,reference_book
from .chart import charts
from .clock import ExchangeClock
from aegis.market import parse_rules
from aegis.domain import Blocked

class IntelligenceService:
    def __init__(self,store,client=None,analyst=None,*,spot_lab=False):
        self.store=store;self.ingestor=Ingestor(store,client);self.analyst=analyst or EvidenceAnalyst(store)
        self.lock=threading.RLock();self.stop=threading.Event();self.job={'running':False,'message':'No research job has run','error':None}
        previous_job=store.state('last_research_job')
        if previous_job:self.job={**previous_job,'running':False,'message':'Previous research interrupted; resume public ingestion' if previous_job.get('running') else previous_job['message']}
        self.paper_mode='PAUSED';self.generation=0;self.prepared={};self.paper=PaperBroker(store,'forward-deterministic',10)
        self.last_feed={};self.last_error=None;self.last_latency_sample={}
        self.pending_candles={};self.needs_resync=False
        self.chart_previews={}
        self.market_clock=ExchangeClock();self.spot=None
        self.assessment_pool=ThreadPoolExecutor(max_workers=1,thread_name_prefix='bounded-evidence')
        self.assessment_future=None;self.approved={}
        if spot_lab:
            from .spot_lab import SpotLab
            self.spot=SpotLab(self)
        self.store.append('strategy_versions',{'id':'trend-pullback-v1','timeframes':['1m','5m'],'holding_horizon_minutes':60,
             'status':'HYPOTHESIS','live_eligible':False,'selection':'No timeframe selected; separate evidence required'},record_id='trend-pullback-v1')

    def research(self,symbols=('BTCUSDT',),intervals=('1m','5m'),lookback_days=14,*,now=None,max_pages=30,stress=True):
        if not 1<=lookback_days<=730 or not symbols or len(symbols)>10 or any(not isinstance(s,str) or not s.isalnum() or not s.endswith('USDT') for s in symbols):raise ValueError('Invalid supported research request')
        if not intervals or any(i not in POLICY for i in intervals):raise ValueError('Research currently evaluates 1m and 5m separately')
        now=time.time() if now is None else now;reports=[]
        self.job={'running':True,'message':'Preparing public research','error':None};self.store.put_state('last_research_job',self.job)
        for symbol in symbols:
            for interval in intervals:
                if self.stop.is_set():return reports
                self.job['message']=f'Public ingestion: {symbol} {interval}; no wallet or model calls'
                try:did,d=self.ingestor.ingest(symbol,interval,now-lookback_days*86400,now,max_pages=max_pages,now=now,cancel=self.stop)
                except PublicDataError as exc:
                    self.job['error']=str(exc)
                    self.store.append('decisions',{'action':'WAIT','reason':'Public ingestion incomplete: '+str(exc),'interval':interval,
                           'source':'data-quality','requested_lookback_days':lookback_days},symbol=symbol,as_of=now)
                    self.store.put_state('last_research_job',self.job);continue
                rows=self.ingestor.load(did)
                if rows:
                    benchmark=None
                    for reference in self.store.records('datasets',symbol='BTCUSDT',limit=20):
                        if reference['body']['interval']==interval and reference['body']['coverage_complete']:
                            benchmark=self.ingestor.load(reference['id']);break
                    p=profile(did,d,rows,benchmark=benchmark);self.store.append('coin_profiles',p,symbol=symbol,as_of=now,version=p['version'])
                    try:self.ingestor.observe(symbol)
                    except Exception:self.last_error='Recent public depth/trades unavailable; historical depth was not fabricated'
                if not d['coverage_complete']:
                    self.job['message']=f'{symbol} {interval}: incomplete coverage; resume ingestion before evaluation';continue
                self.job['message']=f'Causal walk-forward evaluation: {symbol} {interval}'
                normal=evaluate_dataset(self.store,did,rows,d);reports.append(normal)
                if stress:reports.append(evaluate_dataset(self.store,did,rows,d,fee=.002,spread_bps=20,slippage_bps=20))
                self.prepare(did,rows,d)
        self.job['running']=False
        self.job['message']='Research incomplete; inspect coverage/errors and resume' if self.job['error'] else 'Research complete. No validated edge or live approval established.'
        self.store.put_state('last_research_job',self.job)
        return reports

    def prepare(self,did,rows=None,d=None):
        d=d or self.store.get('datasets',did)['body'];rows=rows or self.ingestor.load(did)
        if not d['coverage_complete'] or len(rows)<100:return
        cached=self.store.state('frozen-predictor:'+did)
        if cached and cached['source_fingerprint']==source_fingerprint():
            predictor=Predictor.__new__(Predictor);predictor.__dict__.update(cached['predictor'])
            self.prepared[(d['symbol'],d['interval'])]={'dataset_id':did,'dataset':d,'rows':rows[-200:],'predictor':predictor,'rules':parse_rules(d['metadata'])}
            return
        rules=parse_rules(d['metadata']);stream=examples(d['symbol'],rows,d['interval'],rules['tick'])
        # Freeze predictor at data cutoff. Forward data is never added to this fit automatically.
        lo=rows[0]['open_ms']/1000;hi=rows[-1]['close_ms']/1000;split=lo+(hi-lo)*.8
        train,valid,_=chronological(stream,split,hi,hi)
        predictor=Predictor(train,valid)
        self.store.put_state('frozen-predictor:'+did,{'source_fingerprint':source_fingerprint(),'predictor':predictor.__dict__})
        self.prepared[(d['symbol'],d['interval'])]={'dataset_id':did,'dataset':d,'rows':rows[-200:],'predictor':predictor,'rules':rules}

    def restore(self):
        if self.spot:return self.spot.restore()
        seen=set()
        for r in self.store.records('datasets',limit=100):
            d=r['body'];key=(d['symbol'],d['interval'])
            if key in seen:continue
            seen.add(key)
            if d['interval'] in POLICY:self.prepare(r['id'],d=d)

    def packet(self,symbol,c,f,regime,pred,profile_record,mem,dataset_id,as_of):
        evidence=[dataset_id]+mem['memory_ids']+([profile_record['id']] if profile_record else [])
        return {'prompt_version':'evidence-v1','generation':self.generation,'as_of':as_of,'candidate':{k:c[k] for k in ('id','symbol','interval','entry','stop_loss','take_profit','expires_at','horizon_bars')},
                'features':f,'regime':regime,'prediction':{k:pred[k] for k in ('id','probabilities','expected_net_return','approximate_95pct_interval','sample_size','eligible','abstain_reasons','model_id')},
                'coin_profile':{k:v for k,v in profile_record['body'].items() if k not in ('sampled_regime_counts',)} if profile_record else None,
                'historical_analogues':mem,'position':self.paper.state['position'],
                'risk_constraints':{'paper_balance':self.paper.state['cash'],'allocation':10,'max_position_pct':80,'loss_per_trade':.1,'daily_loss':.5,'one_position':True},
                'data_warnings':['Paper only; predictive edge unvalidated','No historical depth reconstruction'],
                'evidence_ids':evidence,'dataset_id':dataset_id,'model_can_change_probabilities_or_risk':False}

    def on_quote(self,symbol,bid,ask,received_at,*,source_event_at=None,monotonic_received=None,source='stream'):
        started=time.monotonic();bid=float(bid);ask=float(ask)
        if not 0<bid<=ask:return
        now=time.time();self.last_feed[symbol]={'bid':bid,'ask':ask,'received_at':received_at,'source_event_at':source_event_at,'source':source}
        if self.spot:
            market_now=self.market_now()
            age=time.monotonic()-monotonic_received if monotonic_received is not None else now-received_at
            if not 0<=age<=3 or source_event_at is not None and not -(self.market_clock.status().get('uncertainty_seconds') or 0)<=market_now-source_event_at<=3:
                self.pause();self.last_error='Stale public quote; entries paused';return
            self.spot.observe(symbol,bid,market_now);self.flush_candles(market_now)
            if not self.market_clock.status()['ready']:
                self.pause();self.last_error=self.market_clock.status()['reason']
            return
        if not 0<=now-received_at<=3 or source_event_at is not None and not 0<=now-source_event_at<=3:
            self.pause();self.last_error='Stale/backlogged quote; entries paused and local exits await fresh data';return
        previous=self.store.state('quote-observed:'+symbol,0)
        if now-previous>=10:
            self.store.append('observations',{'bid':str(bid),'ask':str(ask),'received_at':received_at,'source_event_at':source_event_at,
                    'source':source,'historical_depth_available':False},symbol=symbol,as_of=now,version='prospective-quote-v1')
            self.store.put_state('quote-observed:'+symbol,now)
        p=self.paper.state['position']
        if p and p['symbol']==symbol:
            # Exit monitoring remains independent of pause, forecast, inference and call budgets.
            self.paper.monitor(bid,now)
        elif not p and (not self.paper.state['equity'] or now-self.paper.state['equity'][-1]['at']>=5):
            # A flat virtual wallet is still an observed equity value, not an empty chart.
            self.paper.mark(bid,now)
        self.flush_candles(now)
        for cid,approval in list(self.approved.items()):
            c=approval['candidate'];reply=approval['reply']
            if now>=min(c['expires_at'],reply['valid_until']):self.approved.pop(cid);continue
            if c['symbol']!=symbol or self.paper_mode!='RUNNING':continue
            prepared=self.prepared.get((symbol,c['interval']))
            if not prepared:continue
            # Material version change invalidates the prior approval, even before expiry.
            if approval['dataset_id']!=prepared['dataset_id'] or approval['cutoff']!=prepared['rows'][-1]['close_ms']/1000:
                self.approved.pop(cid);continue
            book=reference_book(symbol,(bid+ask)/2,prepared['rules'],received_at,0)
            book.update(bid=str(bid),ask=str(ask),spread_bps=(ask-bid)/((ask+bid)/2)*10000)
            result=self.paper.enter(c,book,now)
            self.store.append('decisions',{'action':'BUY' if result['accepted'] else 'WAIT','reason':result.get('reason','Approved paper entry'),
                   'candidate_id':cid,'assessment_id':reply['assessment_id'],'source':'forward-paper','memory_ids':approval['memory_ids']},symbol=symbol,as_of=now)
            self.approved.pop(cid)
        for (sym,interval),prepared in list(self.prepared.items()):
            if sym!=symbol or self.paper_mode!='RUNNING':continue
            policy=POLICY[interval]
            if not 0<=now-received_at<=policy['max_feed_age']:continue
            rows=prepared['rows'][-150:]
            if not rows:continue
            frame_key=f'frame:{symbol}:{interval}'
            if self.store.state(frame_key)==rows[-1]['close_ms']:continue
            self.store.put_state(frame_key,rows[-1]['close_ms'])
            try:f,r=features(rows,interval,now,spread_bps=(ask-bid)/((ask+bid)/2)*10000);c=candidate(symbol,rows,interval,now,ask,prepared['rules']['tick'])
            except ValueError as exc:
                if 'stale' in str(exc) or 'gap' in str(exc):self.needs_resync=True
                self.store.append('decisions',{'action':'WAIT','reason':str(exc),'source':'forward-paper','interval':interval},symbol=symbol,as_of=now);continue
            fid=self.store.append('feature_snapshots',f,symbol=symbol,as_of=now)
            self.store.append('regime_snapshots',r,symbol=symbol,as_of=now)
            if not c['eligible']:
                self.store.append('decisions',{'action':'WAIT','reason':c['reason'],'feature_id':fid,'source':'forward-paper','interval':interval},symbol=symbol,as_of=now);continue
            row={'id':c['id'],'features':f,'regime':r['label'],'candidate':c};pred=prepared['predictor'].predict(row)
            pid=self.store.append('predictions',{**pred,'candidate':c,'feature_id':fid,'status':'PENDING_FORWARD','regime':r['label'],'dataset_ids':[prepared['dataset_id']]},symbol=symbol,as_of=now,record_id=pred['id'])
            mem=retrieve(self.store,f,r['label'],symbol=symbol,as_of=now)
            profiles=self.store.records('coin_profiles',symbol=symbol,at=now,limit=1)
            packet=self.packet(symbol,c,f,r,pred,profiles[0] if profiles else None,mem,prepared['dataset_id'],now)
            # Ordinary deterministic paper baseline; no model call on the quote thread.
            decision={'action':'WAIT','reason':'; '.join(pred['abstain_reasons']),'source':'forward-paper','candidate_id':c['id'],
                      'prediction_id':pid,'memory_ids':mem['memory_ids'],'evidence_ids':packet['evidence_ids'],'interval':interval,
                      'model_status':'Disabled; no inference on feed thread'}
            if pred['eligible']:
                book=reference_book(symbol,(bid+ask)/2,prepared['rules'],received_at,0)
                book.update(bid=str(bid),ask=str(ask),spread_bps=(ask-bid)/((ask+bid)/2)*10000)
                try:
                    sized=self.paper.preflight(c,book,now)
                    decision['preflight_qty']=sized['qty']
                    # Only materially eligible, locally feasible candidates reach the model queue.
                    self.store.put_state('pending_evidence:'+c['id'],packet)
                    decision['reason']='Forecast and local risk eligible; awaiting separately dispatched bounded GPT assessment. No trigger-time inference.'
                except Blocked as exc:decision['reason']='Local risk blocked before model dispatch: '+str(exc)
            self.store.append('decisions',decision,symbol=symbol,as_of=now)
        duration=(time.monotonic()-started)*1000
        if now-self.last_latency_sample.get(symbol,0)<.5:return
        self.last_latency_sample[symbol]=now
        self.store.append('latencies',{'processing_ms':duration,'sampling':'At most two measurements/second/symbol','receive_to_processing_ms':max(0,(started-monotonic_received)*1000) if monotonic_received is not None else None,
             'source_event_at':source_event_at,'received_at':received_at,'completed_candle_at':None,'dispatch_at':None,'ack_at':None,'fill_at':None,
             'clock_offset_uncertainty_ms':None,'network_latency_ms':None,'limitation':'Cross-system latency unknown without clock uncertainty; paper records are not exchange fills'},symbol=symbol,as_of=now)

    def on_chart_candle(self,symbol,interval,bar,received_at,exchange_closed=False):
        """Display preview only; cannot enter research, features or order sizing."""
        if (symbol,interval) not in self.prepared:return
        if self.spot:received_at+=self.market_now()-time.time()
        with self.lock:
            previous=self.chart_previews.get((symbol,interval))
            if previous and (bar['open_ms']<previous['bar']['open_ms'] or
                bar['open_ms']==previous['bar']['open_ms'] and previous['exchange_closed'] and not exchange_closed):return
            self.chart_previews[(symbol,interval)]={'bar':bar,'received_at':received_at,'exchange_closed':exchange_closed}

    def on_candle(self,symbol,interval,bar):
        prepared=self.prepared.get((symbol,interval))
        if not prepared:return
        now=self.market_now()
        if bar['close_ms']/1000>=now:
            if bar['close_ms']/1000-now>30:
                self.pause();self.last_error='Exchange candle is more than 30 seconds ahead of PC time; check clock synchronization';self.needs_resync=True
                return
            # A closed exchange event can precede a slow PC clock. Retain it, but do
            # not expose it to features until local decision time passes its close.
            self.pending_candles[(symbol,interval,bar['open_ms'])]=bar
            return
        rows=prepared['rows']
        if rows and bar['open_ms']<=rows[-1]['open_ms']:return
        if rows and bar['open_ms']-rows[-1]['open_ms']!=INTERVALS[interval]*1000:
            self.pause();self.last_error='Stream gap detected; public REST resynchronization required';self.needs_resync=True;return
        prepared['rows']=(rows+[bar])[-200:]
        self.store.append('observations',{'interval':interval,'close_ms':bar['close_ms'],'received_at':now,'candle':bar,
                          'source':'Validated closed public stream candle'},symbol=symbol,as_of=now,version='prospective-candle-v1',
                          record_id=identity(['forward-candle',symbol,interval,bar['open_ms']]))
        if not self.spot:self.resolve_forward(symbol,interval,prepared['rows'])

    def flush_candles(self,now):
        for key,bar in sorted(list(self.pending_candles.items())):
            if bar['close_ms']/1000<now:
                self.pending_candles.pop(key,None);self.on_candle(key[0],key[1],bar)

    def signal_health(self):
        now=self.market_now()
        return [{'symbol':s,'interval':i,'last_close':p['rows'][-1]['close_ms']/1000 if p['rows'] else None,
                 'age_seconds':max(0,now-p['rows'][-1]['close_ms']/1000) if p['rows'] else None,
                 'fresh':bool(p['rows'] and 0<now-p['rows'][-1]['close_ms']/1000<=INTERVALS[i]*2)}
                for (s,i),p in list(self.prepared.items())]

    def resolve_forward(self,symbol,interval,rows):
        for record in self.store.records('predictions',symbol=symbol,limit=500):
            p=record['body'];c=p.get('candidate')
            if p.get('status')!='PENDING_FORWARD' or not c or c['interval']!=interval:continue
            oid='forward-outcome:'+record['id']
            if self.store.get('prediction_outcomes',oid):continue
            future=[r for r in rows if r['open_ms']/1000>c['data_cutoff']]
            outcome=resolve(c,future)
            if outcome is None:continue
            reference={**outcome,'prediction_id':record['id'],'mode':'FORWARD_PUBLIC_REFERENCE',
                'attribution':['Forecast class differed' if max(p['probabilities'],key=p['probabilities'].get)!=outcome['outcome'] else 'Forecast class matched'],
                'limitation':'Public reference target; paper order fills and fees are tracked separately'}
            self.store.append('prediction_outcomes',reference,symbol=symbol,as_of=outcome['resolved_at'],record_id=oid)
            f=self.store.get('feature_snapshots',p['feature_id'])
            if f:remember(self.store,'observed_fact',{'features':f['body'],'regime':p['regime'],'interval':interval,**outcome},
                           [record['id'],oid,*p['dataset_ids']],symbol=symbol,as_of=outcome['resolved_at'])
            # Review is append-only; no automatic refit, threshold change or promotion.
            remember(self.store,'statistical_finding',{'text':'Resolved forward reference outcome; re-evaluate calibration only on an eligible new evidence set.',
                     'outcome':outcome['outcome'],'net_return':outcome['net_return']},[record['id'],oid],symbol=symbol,as_of=time.time())

    def attach_analyst(self,chat_adapter,exact_selected_model,*,explicit_runtime_authorization=False):
        if not explicit_runtime_authorization:raise ValueError('Separate explicit authorization is required for runtime inference')
        self.analyst=EvidenceAnalyst(self.store,chat_adapter,exact_selected_model,enabled=True)

    def dispatch_pending(self):
        """Called by background worker, never the feed/stop-monitoring thread."""
        if self.spot:return self.spot.tick()
        if self.assessment_future:
            if not self.assessment_future.done():return
            packet,reply,source=self.assessment_future.result();self.assessment_future=None
            if packet['generation']!=self.generation or self.paper_mode!='RUNNING':
                reply={'action':'WAIT','reason':'Assessment invalidated by Pause or feed recovery','candidate_id':packet['candidate']['id']}
            if reply.get('action')=='BUY' and reply.get('valid_until',0)>time.time() and packet['generation']==self.generation and self.paper_mode=='RUNNING':
                c={**packet['candidate'],'created_at':packet['as_of'],'assessment_at':time.time(),'data_cutoff':packet['features']['data_cutoff']}
                self.approved[c['id']]={'candidate':c,'reply':reply,'dataset_id':packet['dataset_id'],
                    'cutoff':packet['features']['data_cutoff'],'memory_ids':packet['historical_analogues']['memory_ids']}
            self.store.append('decisions',{'action':reply.get('action','WAIT'),'reason':reply.get('rationale',reply.get('reason')),
                'candidate_id':packet['candidate']['id'],'memory_ids':packet['historical_analogues']['memory_ids'],
                'evidence_ids':packet['evidence_ids'],'source':'bounded-evidence-'+source},symbol=packet['candidate']['symbol'],as_of=time.time())
        if not self.analyst.economy.enabled or self.paper_mode!='RUNNING':return
        with self.store.lock:
            rows=self.store.db.execute("SELECT key,body FROM state WHERE key LIKE 'pending_evidence:%'").fetchall()
        import json
        packets=sorted([(r['key'],json.loads(r['body'])) for r in rows],key=lambda pair:pair[1]['prediction']['expected_net_return'],reverse=True)
        for key,packet in packets:
            if self.store.state('dispatched:'+key):continue
            self.store.put_state('dispatched:'+key,True)
            if packet['candidate']['expires_at']<=time.time() or packet['generation']!=self.generation:continue
            self.assessment_future=self.assessment_pool.submit(lambda p=packet:(p,*self.analyst.assess(p)));break

    def close(self):
        self.stop.set();self.assessment_pool.shutdown(wait=True,cancel_futures=True)

    def pause(self):
        with self.lock:
            self.paper_mode='PAUSED';self.generation+=1;self.approved.clear()

    def market_now(self):
        return self.market_clock.now() if self.spot else time.time()

    def snapshot(self):
        now=self.market_now();tables={t:self.store.records(t,limit=100) for t in ('datasets','coin_profiles','feature_snapshots','regime_snapshots','predictions','prediction_outcomes','assessments','decisions','trade_episodes','hypotheses','evaluation_runs','memory_items')}
        latencies=[r['body']['processing_ms'] for r in self.store.records('latencies',limit=1000)]
        def percentile(q):
            return sorted(latencies)[min(len(latencies)-1,int((len(latencies)-1)*q))] if latencies else None
        with self.lock:previews=dict(self.chart_previews)
        paper=self.spot.paper if self.spot else self.paper
        analyst=self.spot.analyst if self.spot else self.analyst
        market_charts=charts(self.prepared,previews,self.store.records('trade_episodes',limit=500),paper.state['position'],now,wallet_key=paper.key)
        return {'workspace':'ISOLATED_PAPER','mode':self.paper_mode,'live_eligible':False,'readiness':'NO_VALIDATED_EDGE',
                'production_access':False,'model':analyst.model,'model_usage':analyst.economy.status(),
                'paper':paper.metrics(),'paper_cash':paper.state['cash'],'paper_initial':paper.state['initial'],
                'paper_key':paper.key,'spot_lab':self.spot.snapshot() if self.spot else None,
                'equity_curve':paper.state['equity'][::max(1,len(paper.state['equity'])//300)][-301:],
                'signal_health':self.signal_health(),'deferred_closed_candles':len(self.pending_candles),
                'market_charts':market_charts,
                'feed':{s:{**q,'stale':time.time()-q['received_at']>3} for s,q in self.last_feed.items()},
                'job':dict(self.job),'error':self.last_error,'latency':{'processing_p50_ms':percentile(.5),'processing_p95_ms':percentile(.95),'samples':len(latencies),'network_latency':'Unknown'},
                **tables,'server_time':now}
