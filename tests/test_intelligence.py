import copy
import io
import json
import math
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock,patch
from aegis.domain import Blocked,D
from aegis.intelligence.store import ResearchStore,safe_directory,identity
from aegis.intelligence.data import Ingestor,PublicClient,PublicDataError,parse_bar,aggregate,numeric
from aegis.intelligence.features import features,candidate,KEYS
from aegis.intelligence.forecast import resolve,Predictor,chronological
from aegis.intelligence.memory import remember,retrieve
from aegis.intelligence.model import Economy,EvidenceAnalyst,validate
from aegis.intelligence.paper import PaperBroker,reference_book
from aegis.intelligence.protection import protection_state
from aegis.intelligence.evaluation import evaluate_dataset,inspection,register_holdout
from aegis.intelligence.service import IntelligenceService
from aegis.intelligence.stream import PublicStream,read_frame

def bars(n=400,interval='5m',start=1699999800):
    step=60 if interval=='1m' else 300;start=start//step*step;rows=[]
    for i in range(n):
        price=100+i*.025+math.sin(i/3)*.35;opening=price-.08
        rows.append({'open_ms':(start+i*step)*1000,'close_ms':(start+(i+1)*step)*1000-1,
            'open':str(opening),'high':str(price+.25),'low':str(opening-.25),'close':str(price),'volume':'100'})
    return rows

def metadata(symbol='BTCUSDT'):
    return {'symbol':symbol,'baseAsset':symbol[:-4],'quoteAsset':'USDT','status':'TRADING','isSpotTradingAllowed':True,
      'ocoAllowed':True,'orderTypes':['MARKET','STOP_LOSS','LIMIT_MAKER'],
      'filters':[{'filterType':'LOT_SIZE','minQty':'.00001','maxQty':'10000','stepSize':'.00001'},
       {'filterType':'PRICE_FILTER','minPrice':'.01','maxPrice':'1000000','tickSize':'.01'},
       {'filterType':'MIN_NOTIONAL','minNotional':'5','applyToMarket':True,'avgPriceMins':0}]}

class FakePublic:
    def __init__(self,rows,page=1000):self.rows=rows;self.page=page;self.calls=[];self.fail_at=None
    def get(self,route,**params):
        self.calls.append((route,params))
        if route=='exchangeInfo':return {'symbols':[metadata(params.get('symbol','BTCUSDT'))]}
        if route=='klines':
            if self.fail_at and sum(r=='klines' for r,p in self.calls)==self.fail_at:raise PublicDataError('fixture disconnect')
            result=[r for r in self.rows if params.get('startTime',0)<=r['open_ms']<=params.get('endTime',10**20)]
            return [[r['open_ms'],r['open'],r['high'],r['low'],r['close'],r['volume'],r['close_ms']] for r in result[:min(params.get('limit',1000),self.page)]]
        if route=='depth':return {'lastUpdateId':1,'bids':[['100','2']],'asks':[['100.01','2']]}
        if route=='trades':return []
        raise AssertionError('No private routes allowed')

def proposed(interval='5m',now=None):
    now=time.time() if now is None else now
    return {'id':'fixture-candidate','symbol':'BTCUSDT','entry':'100','stop_loss':'99','take_profit':'103','created_at':now,
            'expires_at':now+90,'interval':interval,'horizon_bars':12 if interval=='5m' else 60}

def training(n=60,start=10000):
    rows=[]
    for i in range(n):
        outcome='TARGET' if i%5<4 else 'STOP';t=start+i*1000
        f={k:.001 for k in KEYS};f.update(interval='5m',spread_bps=None)
        rows.append({'id':str(t),'as_of':t,'features':f,'regime':'trending','candidate':proposed(now=t),
                     'label':{'outcome':outcome,'net_return':.02 if outcome=='TARGET' else -.01,'resolved_at':t+500}})
    return rows

def packet(now):
    c=proposed(now=now);c['expires_at']=now+3600
    return {'as_of':now,'candidate':c,'prediction':{'id':'prediction-one','eligible':True},'evidence_ids':['dataset-one'],
            'features':{'version':'v1','data_cutoff':now-1},'position':None}

class StoreCase(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.folder=Path(self.temp.name)/'isolated'
        self.store=ResearchStore(self.folder);self.addCleanup(self.store.close)

class StorageTests(StoreCase):
    def test_migrations_are_idempotent_and_records_immutable(self):
        rid=self.store.append('decisions',{'action':'WAIT'},record_id='fixed')
        with self.assertRaises(ValueError):self.store.append('decisions',{'action':'BUY'},record_id=rid)
        second=ResearchStore(self.folder)
        try:self.assertEqual(second.db.execute('SELECT COUNT(*) FROM migrations').fetchone()[0],1);self.assertEqual(second.get('decisions',rid)['body']['action'],'WAIT')
        finally:second.close()
    def test_production_and_nonempty_directories_refused_without_opening_files(self):
        with self.assertRaises(ValueError):safe_directory('C:/Users/HP/Music/Aegis-Crypto-Agent/binance-v3/data')
        other=Path(self.temp.name)/'existing';other.mkdir();(other/'unrelated.txt').write_text('keep')
        with self.assertRaises(ValueError):safe_directory(other)
        self.assertEqual((other/'unrelated.txt').read_text(),'keep')

class DataTests(StoreCase):
    def ingest(self,rows,client=None,max_pages=100,now=None):
        start=rows[0]['open_ms']/1000;end=(rows[-1]['close_ms']+1)/1000
        ing=Ingestor(self.store,client or FakePublic(rows));did,d=ing.ingest('BTCUSDT','5m',start,end,now=now or end+1,max_pages=max_pages)
        return ing,did,d
    def test_resume_dedup_corrections_and_frozen_versions(self):
        rows=bars(8);client=FakePublic(rows,page=2);client.fail_at=2
        ing,did,d=self.ingest(rows,client);self.assertFalse(d['coverage_complete']);self.assertEqual(d['bars'],2)
        client.fail_at=None;_,full_id,full=self.ingest(rows,client);self.assertTrue(full['coverage_complete']);self.assertEqual(full['bars'],8)
        old=ing.load(full_id);client.rows[-1]={**client.rows[-1],'close':client.rows[-1]['open']}
        _,new_id,new=self.ingest(rows,client);self.assertEqual(new['corrections_observed'],1)
        self.assertNotEqual(ing.load(new_id)[-1]['close'],old[-1]['close']);self.assertEqual(ing.load(full_id),old)
        self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM candle_heads').fetchone()[0],8)
    def test_gaps_unfinished_and_rate_limit_are_visible(self):
        rows=bars(8);client=FakePublic(rows[:3]+rows[4:]);_,_,d=self.ingest(rows,client)
        self.assertFalse(d['coverage_complete']);self.assertEqual(d['missing_bars'],1)
        row=[rows[-1]['open_ms'],rows[-1]['open'],rows[-1]['high'],rows[-1]['low'],rows[-1]['close'],rows[-1]['volume'],rows[-1]['close_ms']]
        self.assertIsNone(parse_bar(row,'5m',rows[-1]['close_ms']/1000))
        from urllib.error import HTTPError
        client=PublicClient()
        with patch('aegis.intelligence.data.urlopen',side_effect=HTTPError('public',429,'limit',{'Retry-After':'90'},None)):
            with self.assertRaises(PublicDataError):client.get('klines',symbol='BTCUSDT')
        self.assertGreater(client.not_before,time.time()+80)
        with self.assertRaises(PublicDataError):client.get('klines')
    def test_identity_and_private_endpoint_rejected(self):
        client=FakePublic(bars(2));client.get=Mock(return_value={'symbols':[{**metadata(),'quoteAsset':'BUSD'}]})
        with self.assertRaises(PublicDataError):Ingestor(self.store,client).metadata('BTCUSDT',time.time())
        with self.assertRaises(PublicDataError):PublicClient().get('order',symbol='BTCUSDT')
    def test_higher_timeframe_uses_only_complete_contiguous_groups(self):
        rows=bars(16,'1m',1700000100);at=(rows[-1]['close_ms']+1)/1000
        result=aggregate(rows,'1m','5m',at);self.assertEqual(len(result),3)
        changed=rows[:-1]+[{**rows[-1],'close':'999'}];self.assertEqual(result,aggregate(changed,'1m','5m',at))
        self.assertEqual(len(aggregate(rows[:2]+rows[3:],'1m','5m',at)),2)

class CausalityTests(StoreCase):
    def test_replayed_memory_does_not_inflate_analogue_count_or_mix_costs(self):
        f={k:.001 for k in KEYS};f.update(interval='5m',spread_bps=None,data_cutoff=50)
        content={'features':f,'interval':'5m','regime':'trending','resolved_at':100,'outcome':'STOP','net_return':-.01,
                 'costs':{'fee_per_side':.001,'spread_bps':10,'slippage_bps':10}}
        remember(self.store,'observed_fact',content,['run-one'],symbol='BTCUSDT',as_of=100,created_at=100)
        remember(self.store,'observed_fact',content,['run-two'],symbol='BTCUSDT',as_of=100,created_at=110)
        stress={**content,'costs':{'fee_per_side':.002,'spread_bps':20,'slippage_bps':20}}
        remember(self.store,'observed_fact',stress,['stress-run'],symbol='BTCUSDT',as_of=100,created_at=120)
        found=retrieve(self.store,f,'trending',symbol='BTCUSDT',as_of=200)
        self.assertEqual(found['sample_size'],1);self.assertEqual(found['raw_compatible_records'],2)
    def test_future_prices_do_not_change_features_or_candidate(self):
        rows=bars(200);as_of=(rows[149]['close_ms']+1)/1000
        f,r=features(rows,'5m',as_of);c=candidate('BTCUSDT',rows,'5m',as_of,104,'.01')
        altered=copy.deepcopy(rows)
        for b in altered[150:]:b.update(close='10000',high='20000',volume='99999')
        self.assertEqual((f,r),features(altered,'5m',as_of));self.assertEqual(c,candidate('BTCUSDT',altered,'5m',as_of,104,'.01'))
    def test_future_memory_and_superseded_findings_are_excluded(self):
        f={k:.001 for k in KEYS};f.update(interval='5m',spread_bps=None)
        def item(t,label):return remember(self.store,'observed_fact',{'features':f,'interval':'5m','regime':'trending','resolved_at':t,'outcome':label,'net_return':.01},['e'],symbol='BTCUSDT',as_of=t,created_at=t)
        old=item(100,'STOP');future=item(300,'TARGET');at200=retrieve(self.store,f,'trending',symbol='BTCUSDT',as_of=200)
        self.assertEqual(at200['memory_ids'],[old]);self.assertNotIn(future,at200['memory_ids'])
        remember(self.store,'hypothesis','correction',['e'],symbol='BTCUSDT',as_of=150,supersedes=old,created_at=150)
        self.assertEqual(retrieve(self.store,f,'trending',symbol='BTCUSDT',as_of=200)['sample_size'],0)
    def test_created_after_decision_is_invisible_even_if_backdated_asof(self):
        self.store.append('memory_items',{'kind':'observed_fact'},as_of=1,created_at=300,available_at=1)
        self.assertEqual(self.store.records('memory_items',at=200),[])
    def test_target_stop_first_timeout_and_gap_are_consistent(self):
        rows=bars(12);c=proposed(now=rows[0]['open_ms']/1000);c.update(entry='100',stop_loss='99',take_profit='103')
        both=copy.deepcopy(rows);both[0].update(open='100',low='98',high='104',close='100')
        self.assertEqual(resolve(c,both)['outcome'],'STOP')
        both[0].update(open='97',low='96',high='100');self.assertIsNone(resolve(c,both))
        flat=[{**r,'open':'100','low':'99.5','high':'100.5','close':'100'} for r in rows]
        self.assertEqual(resolve(c,flat)['outcome'],'TIMEOUT');self.assertIsNone(resolve(c,flat[:3]))
        gap=flat[:4]+[{**flat[4],'open':'98','low':'97'}]+flat[5:]
        self.assertLess(resolve(c,gap)['exit'],98)
    def test_purging_and_calibration_are_reproducible(self):
        rows=training(100);train,valid,test=chronological(rows,50000,70000,70000)
        self.assertTrue(all(r['label']['resolved_at']<50000 for r in train));self.assertTrue(all(r['label']['resolved_at']<70000 for r in valid))
        p=Predictor(train,valid);q=Predictor(train,valid)
        self.assertEqual(p.predict(test[0]),q.predict(test[0]));self.assertAlmostEqual(sum(p.predict(test[0])['probabilities'].values()),1)
        tiny=Predictor(train[:2],valid[:2]).predict(test[0]);self.assertFalse(tiny['eligible'])
        changed=copy.deepcopy(test[0]);changed['features']['volatility']=100
        self.assertFalse(p.predict(changed)['eligible'])
    def test_holdout_inspection_and_registration_cannot_relabel_seen_history(self):
        d={'symbol':'BTCUSDT','interval':'5m','venue':'Binance Global Spot'}
        with self.assertRaises(ValueError):register_holdout(self.store,90,200,now=100)
        register_holdout(self.store,110,200,now=100)
        self.assertTrue(inspection(self.store,d,110,200,'one',210)['untouched'])
        self.assertFalse(inspection(self.store,d,110,200,'two',220)['untouched'])

class BudgetTests(StoreCase):
    def test_supported_chatgpt_adapter_preserves_model_and_bounded_request(self):
        from aegis.chatgpt import ChatGPT
        adapter=ChatGPT.__new__(ChatGPT);adapter.models=Mock(return_value=[{'slug':'gpt-6-sol','name':'Account supplied GPT-6 Sol'}]);adapter._infer=Mock(return_value=({'fixture':True},{}))
        adapter.assess_evidence(packet(10000),'gpt-6-sol',{'type':'object'})
        body,timeout=adapter._infer.call_args.args
        self.assertEqual(body['model'],'gpt-6-sol');self.assertEqual(body['max_output_tokens'],500)
        self.assertEqual(body['reasoning']['effort'],'low');self.assertFalse(body['store']);self.assertTrue(body['stream']);self.assertNotIn('service_tier',body)
        with self.assertRaises(Blocked):adapter.assess_evidence({},'gpt-6-astra',{})

    def test_restart_hourly_cooldown_failures_no_retry(self):
        gate=Economy(self.store,enabled=True);rid=gate.reserve('a','BTCUSDT',10000);gate.finish(rid,error='timeout')
        gate2=Economy(self.store,enabled=True)
        for key,symbol in (('a','BTCUSDT'),('b','BTCUSDT')):
            with self.assertRaises(Blocked):gate2.reserve(key,symbol,10001)
        second=gate2.reserve('b','ETHUSDT',10001);gate2.finish(second)
        with self.assertRaises(Blocked):gate2.reserve('c','SOLUSDT',10002)
        self.assertEqual(gate2.status(10002)['calls_rolling_hour'],2)
    def test_daily_budget_survives_new_connection(self):
        gate=Economy(self.store,enabled=True,hourly=100,daily=3,cooldown=0)
        for i in range(3):rid=gate.reserve(str(i),'BTCUSDT',10000+i);gate.finish(rid)
        second=ResearchStore(self.folder)
        try:
            with self.assertRaises(Blocked):Economy(second,enabled=True,hourly=100,daily=3,cooldown=0).reserve('next','BTCUSDT',10004)
        finally:second.close()
    def test_concurrent_workers_cannot_overreserve(self):
        def reserve(i):
            s=ResearchStore(self.folder)
            try:
                e=Economy(s,enabled=True,cooldown=0)
                try:rid=e.reserve(str(i),'BTCUSDT',10000);e.finish(rid);return 1
                except Blocked:return 0
            finally:s.close()
        with ThreadPoolExecutor(max_workers=8) as pool:counts=list(pool.map(reserve,range(16)))
        self.assertLessEqual(sum(counts),2);self.assertGreaterEqual(sum(counts),1)
    def test_validated_cache_and_changed_input_invalidation(self):
        now=[10000];p=packet(now[0]);adapter=Mock()
        def response(p,*args,**kw):return {'assessment_id':'assessment-one','candidate_id':p['candidate']['id'],'symbol':'BTCUSDT',
            'as_of':p['as_of'],'valid_until':p['candidate']['expires_at'],'action':'BUY','prediction_id':p['prediction']['id'],
            'evidence_ids':['dataset-one'],'reason_codes':['EVIDENCE_REVIEWED'],'rationale':'Fixture accepts fixed evidence.',
            'invalidation_conditions':['Expiry'],'limitations':['Synthetic test only']},{'input_tokens':100,'output_tokens':80,'output_tokens_details':{'reasoning_tokens':40}}
        adapter.assess_evidence.side_effect=response
        analyst=EvidenceAnalyst(self.store,adapter,'gpt-6-sol',enabled=True,clock=lambda:now[0])
        self.assertEqual(analyst.assess(p)[1],'MODEL');self.assertEqual(analyst.assess(p)[1],'CACHE');self.assertEqual(adapter.assess_evidence.call_count,1)
        changed=copy.deepcopy(p);changed['features']['version']='v2'
        self.assertEqual(analyst.assess(changed)[0]['action'],'WAIT');self.assertEqual(adapter.assess_evidence.call_count,1)
    def test_disabled_wrong_model_malformed_expired_and_outages_fail_closed(self):
        p=packet(10000);adapter=Mock()
        for enabled,model in ((False,'gpt-6-sol'),(True,'gpt-6-astra'),(True,None)):
            analyst=EvidenceAnalyst(self.store,adapter,model,enabled=enabled,clock=lambda:10000)
            self.assertEqual(analyst.assess(p)[0]['action'],'WAIT')
        adapter.assess_evidence.assert_not_called()
        adapter.assess_evidence.side_effect=TimeoutError()
        analyst=EvidenceAnalyst(self.store,adapter,'gpt-6-sol',enabled=True,clock=lambda:10000)
        self.assertEqual(analyst.assess(p)[0]['action'],'WAIT');self.assertEqual(analyst.assess(p)[0]['action'],'WAIT');self.assertEqual(adapter.assess_evidence.call_count,1)
        with self.assertRaises(Blocked):validate({'action':'BUY'},p,10000)

class PaperTests(StoreCase):
    def test_bounded_equity_marks_keep_drawdown_and_local_risk_is_pure(self):
        broker=PaperBroker(self.store,'bounded',10);c=proposed(now=10000);before=copy.deepcopy(broker.state)
        broker.preflight(c,self.book(),10000);self.assertEqual(before,broker.state)
        broker.enter(c,self.book(),10000)
        for i in range(100):broker.mark(90 if i==50 else 100,10000+i/100)
        self.assertEqual(len(broker.state['equity']),1);self.assertGreater(D(broker.metrics()['max_drawdown']),0)

    def book(self,now=10000):
        from aegis.market import parse_rules
        return reference_book('BTCUSDT',100,parse_rules(metadata()),now,0)
    def test_five_dollar_skips_and_ten_respects_costs_and_restart(self):
        now=10000;c=proposed(now=now);book=self.book(now)
        small=PaperBroker(self.store,'five',5);self.assertFalse(small.enter(c,book,now)['accepted']);self.assertIn('minimum',str(small.state['skip_reasons']))
        broker=PaperBroker(self.store,'ten',10);self.assertTrue(broker.enter(c,book,now)['accepted'])
        self.assertLess(D(broker.state['cash']),D(10));self.assertLessEqual(D(broker.state['position']['original_cost']),D('8.02'))
        restarted=PaperBroker(self.store,'ten',10);self.assertFalse(restarted.enter(c,book,now)['accepted'])
        restarted.monitor('98',now+1);self.assertIsNone(restarted.state['position']);self.assertLess(D(restarted.state['cash']),D(10))
        self.assertEqual(restarted.metrics()['closed_trades'],1)
    def test_partial_fill_dust_stays_visible_and_cannot_average_down(self):
        c=proposed(now=10000);broker=PaperBroker(self.store,'partial',10)
        self.assertTrue(broker.enter(c,self.book(),10000,fill_fraction='.25')['accepted'])
        c2={**c,'id':'second'};self.assertFalse(broker.enter(c2,self.book(),10000)['accepted'])
        result=broker.monitor(95,10001);self.assertFalse(result['closed']);self.assertIsNotNone(broker.metrics()['open_position'])
        self.assertLess(D(broker.metrics()['net_total']),0);self.assertIsNone(broker.metrics()['win_rate'])
    def test_slow_model_exhaustion_and_paused_entries_do_not_block_tick_exit(self):
        service=IntelligenceService(self.store,FakePublic([]));self.addCleanup(service.close)
        service.paper.enter(proposed(now=time.time()),self.book(time.time()),time.time())
        service.paper_mode='PAUSED';service.analyst.economy.enabled=False
        blocker=threading.Event();future=service.assessment_pool.submit(lambda:blocker.wait(2))
        try:
            service.on_quote('BTCUSDT',98,98.01,time.time())
            self.assertFalse(future.done())
        finally:blocker.set()
        self.assertIsNone(service.paper.state['position']);self.assertEqual(service.analyst.economy.status()['calls_rolling_24h'],0)
    def test_partial_exit_accounts_costs_and_keeps_dust_open(self):
        broker=PaperBroker(self.store,'partial-exit',10);broker.enter(proposed(now=10000),self.book(),10000)
        result=broker.monitor(104,10001,fill_fraction='.8');self.assertFalse(result['closed'])
        self.assertGreater(D(broker.state['position']['qty']),0);self.assertNotEqual(D(broker.metrics()['net_realized']),0)
        broker.monitor(98,10002);self.assertIsNotNone(broker.metrics()['open_position']);self.assertIsNone(broker.metrics()['win_rate'])
    def test_stale_expired_and_price_chasing_are_rejected(self):
        broker=PaperBroker(self.store,'stale',10);c=proposed('1m',10000)
        self.assertFalse(broker.enter(c,self.book(9990),10000)['accepted'])
        self.assertFalse(broker.enter(c,self.book(10100),10100)['accepted'])
        shifted=self.book();shifted['ask']='100.5'
        self.assertFalse(broker.enter(c,shifted,10000)['accepted'])

class StreamProtectionTests(StoreCase):
    def test_pause_invalidates_a_completed_but_uncollected_assessment(self):
        from concurrent.futures import Future
        service=IntelligenceService(self.store,FakePublic([]));self.addCleanup(service.close);service.paper_mode='RUNNING'
        p=packet(time.time());p.update(generation=service.generation,dataset_id='fixture',historical_analogues={'memory_ids':[]})
        reply={'action':'BUY','assessment_id':'fixture-paused','valid_until':time.time()+90}
        future=Future();future.set_result((p,reply,'MODEL'));service.assessment_future=future
        service.pause();service.dispatch_pending();self.assertEqual(service.approved,{})
        self.assertEqual(self.store.records('decisions')[0]['body']['action'],'WAIT')
    def test_paper_timings_are_explicit_and_never_claim_exchange_fills(self):
        from aegis.market import parse_rules
        broker=PaperBroker(self.store,'timing',10);now=10000
        broker.enter(proposed(now=now),reference_book('BTCUSDT',100,parse_rules(metadata()),now,0),now)
        timing=self.store.records('latencies')[0]['body']
        self.assertEqual(timing['event_type'],'PAPER_ENTRY');self.assertGreaterEqual(timing['fill_at'],timing['dispatch_at'])
        self.assertIn('not exchange',timing['time_basis'])
    def test_disconnect_backoff_resynchronizes_and_never_rearms(self):
        service=Mock();service.paper_mode='RUNNING';stream=PublicStream(service,['BTCUSDT']);delays=[]
        class Stop:
            def is_set(self):return len(delays)>=4
            def wait(self,delay):delays.append(delay)
        stream.stop=Stop()
        def resync():service.paper_mode='PAUSED'
        stream.resync=Mock(side_effect=resync);stream.open=Mock(side_effect=ConnectionError('fixture offline'))
        stream.run();self.assertEqual(delays,[2,4,8,16]);self.assertEqual(service.paper_mode,'PAUSED');self.assertEqual(stream.resync.call_count,4)
    def test_unfeasible_candidate_is_blocked_before_model_queue(self):
        from aegis.market import parse_rules
        now=time.time();row={**bars(1)[0],'close_ms':int(now*1000)-1000}
        c={**proposed(now=now),'eligible':True,'data_cutoff':row['close_ms']/1000}
        f={k:.001 for k in KEYS};f.update(interval='5m',data_cutoff=c['data_cutoff'])
        pred={'id':'unfeasible-pred','probabilities':{'TARGET':.8,'STOP':.1,'TIMEOUT':.1},'expected_net_return':.01,
              'approximate_95pct_interval':[.001,.02],'sample_size':60,'eligible':True,'abstain_reasons':[],'model_id':'fixture'}
        service=IntelligenceService(self.store,FakePublic([]));self.addCleanup(service.close)
        service.paper=PaperBroker(self.store,'five-forward',5);service.paper_mode='RUNNING'
        service.prepared[('BTCUSDT','5m')]={'dataset_id':'fixture','rows':[row],'rules':parse_rules(metadata()),'predictor':Mock(predict=Mock(return_value=pred))}
        with patch('aegis.intelligence.service.features',return_value=(f,{'label':'trending','interval':'5m'})),patch('aegis.intelligence.service.candidate',return_value=c):service.on_quote('BTCUSDT',100,100.01,time.time())
        self.assertIsNone(self.store.state('pending_evidence:'+c['id']))
        self.assertIn('Local risk blocked before model dispatch',self.store.records('decisions')[0]['body']['reason'])
    def test_quote_telemetry_is_sampled_and_timeframe_is_explicit(self):
        service=IntelligenceService(self.store,FakePublic([]));self.addCleanup(service.close)
        for i in range(100):
            now=10000+i/100
            with patch('aegis.intelligence.service.time.time',return_value=now):service.on_quote('BTCUSDT',100,100.01,now)
        self.assertLessEqual(len(self.store.records('latencies')),2)
        rows=bars(100);_,r=features(rows,'5m',(rows[-1]['close_ms']+1)/1000);self.assertEqual(r['interval'],'5m')
    def test_metadata_disconnect_is_durable_and_clear_after_restart(self):
        client=Mock();client.get.side_effect=PublicDataError('fixture offline')
        service=IntelligenceService(self.store,client);self.addCleanup(service.close)
        self.assertEqual(service.research(['BTCUSDT'],['5m'],1),[])
        self.assertEqual(self.store.state('last_research_job')['error'],'fixture offline')
        restored=IntelligenceService(self.store,client);self.addCleanup(restored.close)
        self.assertEqual(restored.job['error'],'fixture offline');self.assertFalse(restored.job['running'])
    def test_frozen_artifact_restore_avoids_research_or_model_calls(self):
        from aegis.intelligence.store import source_fingerprint
        rows=bars(160);ing=Ingestor(self.store,FakePublic(rows));end=(rows[-1]['close_ms']+1)/1000
        did,d=ing.ingest('BTCUSDT','5m',rows[0]['open_ms']/1000,end,now=end+1)
        service=IntelligenceService(self.store,FakePublic([]));self.addCleanup(service.close)
        predictor=Predictor(training(40),training(20,100000))
        self.store.put_state('frozen-predictor:'+did,{'source_fingerprint':source_fingerprint(),'predictor':predictor.__dict__})
        with patch('aegis.intelligence.service.examples',side_effect=AssertionError('No repeated replay on unchanged dataset')):service.prepare(did)
        self.assertEqual(service.prepared[('BTCUSDT','5m')]['predictor'].id,predictor.id)
        self.assertEqual(service.analyst.economy.status()['calls_rolling_24h'],0)
    def test_complete_evidence_to_paper_exit_and_outcome_memory_pipeline(self):
        from aegis.market import parse_rules
        now=time.time();closed_ms=int(now//300*300*1000)-1
        row={**bars(1)[0],'open_ms':closed_ms-299999,'close_ms':closed_ms}
        c=proposed(now=now);c['data_cutoff']=closed_ms/1000
        f={k:.001 for k in KEYS};f.update(interval='5m',version='fixture-feature-v1',data_cutoff=c['data_cutoff'],as_of=now)
        pred={'id':'forward-test-prediction','probabilities':{'TARGET':.8,'STOP':.15,'TIMEOUT':.05},'expected_net_return':.014,
              'approximate_95pct_interval':[.005,.025],'sample_size':60,'eligible':True,'abstain_reasons':[],
              'model_id':'fixture-statistical-model','interval':'5m','training_n':100,'validation_n':30}
        adapter=Mock()
        def response(packet,*a,**kw):
            return {'assessment_id':'forward-assessment','candidate_id':c['id'],'symbol':'BTCUSDT','as_of':packet['as_of'],
             'valid_until':c['expires_at'],'action':'BUY','prediction_id':pred['id'],'evidence_ids':['fixture-dataset'],
             'reason_codes':['FIXTURE_EVIDENCE'],'rationale':'Controlled synthetic acceptance, not a profitability result.',
             'invalidation_conditions':['Expiry or data change'],'limitations':['Synthetic fixture only']},{'input_tokens':100,'output_tokens':60}
        adapter.assess_evidence.side_effect=response
        analyst=EvidenceAnalyst(self.store,adapter,'gpt-6-sol',enabled=True)
        service=IntelligenceService(self.store,FakePublic([]),analyst);self.addCleanup(service.close)
        predictor=Mock();predictor.predict.return_value=pred
        service.prepared[('BTCUSDT','5m')]={'dataset_id':'fixture-dataset','dataset':{},'rows':[row],'predictor':predictor,'rules':parse_rules(metadata())}
        service.paper_mode='RUNNING'
        with patch('aegis.intelligence.service.features',return_value=(f,{'label':'trending'})),patch('aegis.intelligence.service.candidate',return_value={**c,'eligible':True}):
            service.on_quote('BTCUSDT',100,100.01,time.time())
            service.dispatch_pending();service.assessment_future.result(timeout=2);service.dispatch_pending()
            self.assertIn(c['id'],service.approved)
            service.on_quote('BTCUSDT',100,100.01,time.time())
            self.assertIsNotNone(service.paper.state['position'])
            service.paper_mode='PAUSED';service.on_quote('BTCUSDT',98,98.01,time.time())
        self.assertIsNone(service.paper.state['position']);self.assertEqual(adapter.assess_evidence.call_count,1)
        future=[{**row,'open_ms':closed_ms+1,'close_ms':closed_ms+300000,'open':'100','low':'98','high':'100.5','close':'98'}]
        service.resolve_forward('BTCUSDT','5m',future)
        service.resolve_forward('BTCUSDT','5m',future)
        outcomes=self.store.records('prediction_outcomes');self.assertEqual(len(outcomes),1);self.assertEqual(outcomes[0]['body']['outcome'],'STOP')
        self.assertTrue(self.store.records('memory_items'));self.assertTrue(self.store.records('decisions'));self.assertEqual(len(self.store.records('trade_episodes')),2)

    def test_paper_http_never_exposes_activation_or_private_data(self):
        import re
        from urllib.request import Request,urlopen
        from urllib.error import HTTPError
        from paper_server import server
        service=IntelligenceService(self.store,FakePublic([]));self.addCleanup(service.close)
        http=server(service,0);thread=threading.Thread(target=http.serve_forever,daemon=True);thread.start()
        self.addCleanup(http.server_close);self.addCleanup(http.shutdown)
        base='http://127.0.0.1:'+str(http.server_address[1])
        with urlopen(base) as response:html=response.read().decode()
        self.assertIn('aegis-paper-workspace',html);token=__import__('re').search(r'name="aegis-session" content="([^"]+)"',html).group(1)
        with self.assertRaises(HTTPError):urlopen(base+'/api/intelligence')
        with urlopen(Request(base+'/api/intelligence',headers={'X-Aegis-Token':token})) as response:body=json.load(response)
        self.assertFalse(body['production_access']);self.assertFalse(body['model_usage']['enabled']);self.assertNotIn('account',body)
        for path in ('arm','binance/connect','model/enable'):
            with self.assertRaises(HTTPError):urlopen(Request(base+'/api/'+path,data=b'{}',headers={'X-Aegis-Token':token,'Content-Type':'application/json'}))
        with self.assertRaises(HTTPError):urlopen(Request(base+'/api/paper/pause',data=b'{}',headers={'X-Aegis-Token':token,'Content-Type':'application/json','Origin':'https://foreign.invalid'}))

    def test_moving_refresh_resumes_from_recent_head_not_start(self):
        rows=bars(10);client=FakePublic(rows);ing=Ingestor(self.store,client);start=rows[0]['open_ms']/1000
        first_end=(rows[7]['close_ms']+1)/1000
        ing.ingest('BTCUSDT','5m',start,first_end,now=first_end+1)
        client.calls.clear();end=(rows[-1]['close_ms']+1)/1000
        _,d=ing.ingest('BTCUSDT','5m',start+300,end,now=end+1)
        page=next(p for r,p in client.calls if r=='klines')
        self.assertGreater(page['startTime'],rows[1]['open_ms']);self.assertTrue(d['coverage_complete'])
    def test_duplicates_and_unfinished_candles_never_become_signals(self):
        service=Mock();stream=PublicStream(service,['BTCUSDT']);event={'s':'BTCUSDT','u':1,'b':'100','a':'100.01'}
        stream.handle(event);stream.handle(event);service.on_quote.assert_called_once()
        event={'e':'kline','s':'BTCUSDT','k':{'i':'1m','x':False,'t':0,'T':59999,'o':'100','h':'101','l':'99','c':'100','v':'1'}};stream.handle(event);service.on_candle.assert_not_called()
    def test_frames_are_bounded_and_ping_is_recognized(self):
        class SocketFixture(io.BytesIO):
            def recv(self,n):return self.read(n)
        self.assertEqual(read_frame(SocketFixture(b'\x89\x01x')),(True,9,b'x'))
        with self.assertRaises(ConnectionError):read_frame(SocketFixture(b'\x81\xff'+b'\xff'*8))
    def test_authoritative_list_and_both_children_required(self):
        intent={'status':'ACCEPTED'};oco={'symbol':'BTCUSDT','listOrderStatus':'EXECUTING','listStatusType':'EXEC_STARTED','orders':[{'orderId':1},{'orderId':2}]}
        children=[{'orderId':1,'symbol':'BTCUSDT','side':'SELL','type':'STOP_LOSS','status':'NEW'},
                  {'orderId':2,'symbol':'BTCUSDT','side':'SELL','type':'LIMIT_MAKER','status':'NEW'}]
        self.assertEqual(protection_state(intent=intent,order_list=oco,children=[]),'UNKNOWN')
        self.assertEqual(protection_state(intent=intent,order_list=oco,children=children),'CONFIRMED_EXCHANGE_HELD')
        children[0]['status']='FILLED';self.assertEqual(protection_state(intent=intent,order_list=oco,children=children),'UNKNOWN')
    def test_empty_research_is_not_promoted_and_survives_restart(self):
        rows=bars(160);client=FakePublic(rows);ing=Ingestor(self.store,client)
        did,d=ing.ingest('BTCUSDT','5m',rows[0]['open_ms']/1000,(rows[-1]['close_ms']+1)/1000,now=(rows[-1]['close_ms']+1)/1000+1)
        report=evaluate_dataset(self.store,did,ing.load(did),d)
        self.assertFalse(report['release']['live_eligible']);self.assertEqual(report['release']['status'],'NO_VALIDATED_EDGE')
        second=ResearchStore(self.folder)
        try:self.assertEqual(second.get('evaluation_runs',report['id'])['body'],report)
        finally:second.close()

if __name__=='__main__':unittest.main()
