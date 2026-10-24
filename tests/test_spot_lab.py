import copy
import tempfile
import threading
import time
import unittest
from concurrent.futures import Future
from types import SimpleNamespace
from unittest.mock import Mock,patch
from aegis.domain import D,Blocked
from aegis.market import parse_rules
from aegis.intelligence.store import ResearchStore
from aegis.intelligence.clock import ExchangeClock
from aegis.intelligence.spot_paper import SpotPaper
from aegis.intelligence.spot_strategy import propose,STRATEGIES
from aegis.intelligence.spot_model import SpotAnalyst,validate
from aegis.intelligence.service import IntelligenceService
from aegis.intelligence.paper import reference_book
from test_intelligence import metadata,proposed,bars,FakePublic


class ClockTests(unittest.TestCase):
    def test_slow_pc_uses_exchange_anchor_without_changing_pc(self):
        c=ExchangeClock();c.update(100,87,87.2,50,50.2)
        with patch('time.monotonic',return_value=50.5),patch('time.time',return_value=87.5):
            self.assertAlmostEqual(c.now(),100.4);self.assertTrue(c.status()['ready'])
            self.assertAlmostEqual(c.status()['offset_seconds'],12.9)
    def test_uncertain_sample_and_clock_jump_fail_closed(self):
        c=ExchangeClock()
        with self.assertRaises(ValueError):c.update(100,87,90,50,53)
        c.update(100,87,87.2,50,50.2)
        with patch('time.monotonic',return_value=50.5),patch('time.time',return_value=90):self.assertFalse(c.status()['ready'])
    def test_expired_clock_sample_cannot_authorize_entries(self):
        c=ExchangeClock();c.update(100,87,87.2,50,50.2)
        with patch('time.monotonic',return_value=300),patch('time.time',return_value=337):self.assertFalse(c.status()['ready'])


class SpotAccountingTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.store=ResearchStore(self.tmp.name);self.now=time.time()
        self.b=SpotPaper(self.store);self.c=proposed('1m',self.now)
        self.c.update(holding_minutes=30,strategy_version='fixture')
        self.book=reference_book('BTCUSDT',100,parse_rules(metadata()),self.now,0)
    def tearDown(self):self.store.close();self.tmp.cleanup()
    def test_base_fee_is_withheld_without_second_cash_charge_and_restart_preserves_assets(self):
        r=self.b.enter(self.c,self.book,self.now);self.assertTrue(r['accepted'],r)
        e=self.store.records('trade_episodes')[0]['body'];gross=D(e['filled_qty']);entry=D(r['position']['entry'])
        self.assertEqual(D(self.b.state['cash']),D(10)-gross*entry)
        received=gross-D(e['commission_quantity'])
        self.assertEqual(received,D(e['sellable_qty'])+D(e['dust_qty']))
        p=r['position'];dust=self.b.state['dust']['BTCUSDT']
        self.assertEqual(D(p['cost'])+D(dust['cost']),gross*entry)
        self.assertLessEqual(D(p['risk_reserved']),D('.10'))
        restored=SpotPaper(self.store);self.assertEqual(restored.state['dust'],self.b.state['dust'])
        self.assertFalse(restored.enter(self.c,self.book,self.now)['accepted'])
    def test_exit_cash_and_cost_basis_reconcile_independently(self):
        self.assertTrue(self.b.enter(self.c,self.book,self.now)['accepted']);p=copy.deepcopy(self.b.state['position'])
        cash=D(self.b.state['cash']);proceeds=D(p['qty'])*D(p['take_profit'])*(1-self.b.slip)*(1-self.b.fee)
        self.b.monitor('104',self.now+1)
        self.assertEqual(D(self.b.state['cash']),cash+proceeds)
        self.assertEqual(D(self.b.state['realized']).quantize(D('.00000001')),(proceeds-D(p['original_cost'])).quantize(D('.00000001')))
        self.assertIsNone(self.b.state['position']);self.assertTrue(self.b.state['dust'])
    def test_other_coin_quote_does_not_reprice_the_open_position(self):
        self.b.enter(self.c,self.book,self.now)
        self.b.observe('BTCUSDT','100',self.now+6);before=self.b.metrics()['marked_equity']
        self.b.observe('DOGEUSDT','.07',self.now+12)
        self.assertEqual(self.b.metrics()['marked_equity'],before)
    def test_coarse_quantity_dust_reduces_size_instead_of_inflating_balance(self):
        m=metadata('DOGEUSDT');m['filters'][0].update(stepSize='1',minQty='1')
        m['filters'][1].update(tickSize='.00001',minPrice='.00001')
        m['filters'][2]['minNotional']='1'
        book=reference_book('DOGEUSDT','.076',parse_rules(m),self.now,0)
        c={**self.c,'symbol':'DOGEUSDT','entry':'.076','stop_loss':'.07562','take_profit':'.086'}
        r=self.b.enter(c,book,self.now);self.assertTrue(r['accepted'],r)
        self.assertLess(D(r['position']['qty'])*D(r['position']['entry']),D('8'))
        self.assertLessEqual(D(r['position']['risk_reserved']),D('.1'))
    def test_minimum_size_is_never_bypassed_for_five_dollars(self):
        b=SpotPaper(self.store,'five-fee-aware',5)
        self.assertFalse(b.enter(self.c,self.book,self.now)['accepted']);self.assertEqual(b.state['cash'],'5')
    def test_stale_expired_and_unprotectable_partial_fill_are_rejected(self):
        self.assertFalse(self.b.enter(self.c,self.book,self.now+100)['accepted'])
        self.assertFalse(self.b.enter(self.c,self.book,self.now,fill_fraction='.1')['accepted'])
        self.assertIsNone(self.b.state['position']);self.assertEqual(self.b.state['cash'],'10')
    def test_three_losses_and_daily_trade_limits_remain_in_code(self):
        self.b.state['daily_date']=time.strftime('%Y-%m-%d',time.gmtime(self.now));self.b.state['consecutive_losses']=3;self.b._commit()
        self.assertFalse(self.b.enter(self.c,self.book,self.now)['accepted'])
        self.b.state['consecutive_losses']=0;self.b.state['daily_entries']=5;self.b._commit()
        self.assertFalse(self.b.enter(self.c,self.book,self.now)['accepted'])

    def test_residual_exposure_is_capped_and_distinct_from_zero_recovery_stress(self):
        m=metadata('DOGEUSDT');m['filters'][0].update(stepSize='1',minQty='1')
        m['filters'][1].update(tickSize='.00001',minPrice='.00001');m['filters'][2]['minNotional']='1'
        book=reference_book('DOGEUSDT','.076',parse_rules(m),self.now,0)
        c={**self.c,'symbol':'DOGEUSDT','entry':'.076','stop_loss':'.07562','take_profit':'.07714'}
        normal=self.b.preflight(c,book,self.now)
        self.assertLessEqual(D(normal['risk']),D('.1'));self.assertLessEqual(D(normal['residual_value']),D('.1'))
        self.assertGreater(D(normal['risk_if_residual_zero']),D(normal['risk']))
        strict=SpotPaper(self.store,'stress-fee-aware',10,dust_stress=True)
        self.assertFalse(strict.enter(c,book,self.now)['accepted'])

    def test_old_residual_combines_with_next_fill_and_total_basis_is_preserved(self):
        m=metadata('DOGEUSDT');m['filters'][0].update(stepSize='1',minQty='1')
        m['filters'][1].update(tickSize='.00001',minPrice='.00001');m['filters'][2]['minNotional']='1'
        book=reference_book('DOGEUSDT','.076',parse_rules(m),self.now,0)
        c={**self.c,'symbol':'DOGEUSDT','entry':'.076','stop_loss':'.07562','take_profit':'.07714'}
        self.assertTrue(self.b.enter(c,book,self.now)['accepted']);self.b.monitor('.078',self.now+1)
        prior=copy.deepcopy(self.b.state['dust']['DOGEUSDT']);later=self.now+1000
        c.update(id='second-dust-fill',created_at=later,expires_at=later+30);book['received_at']=later
        self.assertTrue(self.b.enter(c,book,later)['accepted'])
        e=self.store.records('trade_episodes')[0]['body'];p=self.b.state['position'];dust=self.b.state['dust']['DOGEUSDT']
        expected=D(prior['qty'])+D(e['filled_qty'])-D(e['commission_quantity'])
        self.assertEqual(D(p['qty'])+D(dust['qty']),expected)
        self.assertEqual(D(p['cost'])+D(dust['cost']),D(e['filled_qty'])*D(p['entry'])+D(prior['cost']))

    def test_large_btc_residual_cannot_pass_the_small_wallet_exposure_cap(self):
        book=reference_book('BTCUSDT','80000',parse_rules(metadata()),self.now,0)
        c={**self.c,'entry':'80000','stop_loss':'79600','take_profit':'81200'}
        self.assertFalse(self.b.enter(c,book,self.now)['accepted']);self.assertEqual(self.b.state['cash'],'10')

    def test_large_equity_history_does_not_change_sizing_or_mutate_day_state(self):
        before=self.b.preflight(self.c,self.book,self.now)
        day=self.b.state['daily_date']
        self.b.state['equity']=[{'at':i,'value':'10','open_position':False} for i in range(10000)]
        after=self.b.preflight(self.c,self.book,self.now)
        self.assertEqual(before,after);self.assertEqual(self.b.state['daily_date'],day)


class StrategyTests(unittest.TestCase):
    def frames(self):
        def trend(interval):
            r=bars(100,interval)
            for i,b in enumerate(r):
                c=100+i*.1;b.update(open=str(c-.02),high=str(c+.05),low=str(c-.05),close=str(c),volume='100')
            return r
        one,five=trend('1m'),trend('5m');now=max(one[-1]['close_ms'],five[-1]['close_ms'])/1000+1
        # Align both completed windows at the same prospective decision time.
        for rows,step in ((one,60),(five,300)):
            end=int(now)//step*step
            for i,b in enumerate(rows):b['open_ms']=(end-(100-i)*step)*1000;b['close_ms']=b['open_ms']+step*1000-1
        now=one[-1]['close_ms']/1000+1
        return one,five,now
    def test_fixed_pullback_and_breakout_proposals_have_immutable_barriers(self):
        a,b,now=self.frames();a[-2].update(open='108.92',high='109.0',low='108.70',close='108.95')
        a[-1].update(open='109.05',high='109.30',low='109.0',close='109.20',volume='140')
        rr=propose('BTCUSDT',a,b,now,'109.20','.01')
        self.assertEqual([r['strategy_version'] for r in rr],list(STRATEGIES));self.assertTrue(rr[0]['eligible'],rr[0])
        p=rr[0];self.assertEqual(D(p['take_profit'])-D(p['entry']),3*(D(p['entry'])-D(p['stop_loss'])))
        a[-2].update(open='109.7',high='110.3',low='109.65',close='110.2',volume='150')
        a[-1].update(open='109.8',high='110.2',low='109.64',close='110.0',volume='100')
        self.assertTrue(propose('BTCUSDT',a,b,now,'110','.01')[1]['eligible'])
    def test_future_and_gapped_candles_never_become_signals(self):
        a,b,now=self.frames();a[-1]['close_ms']=int(now*1000)+100
        with self.assertRaises(ValueError):propose('BTCUSDT',a,b,now,110,'.01')
        a,b,now=self.frames();a.pop(-8)
        with self.assertRaises(ValueError):propose('BTCUSDT',a,b,now,110,'.01')
    def test_old_completed_signal_is_not_revived_on_start(self):
        a,b,now=self.frames();r=propose('BTCUSDT',a,b,now+35,110,'.01')
        self.assertTrue(all(not c['eligible'] for c in r));self.assertIn('expired',r[0]['reason'])


class GPTTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.store=ResearchStore(self.tmp.name);self.now=time.time()
        self.a=SpotAnalyst(self.store,lambda:self.now);c=proposed('1m',self.now)
        self.packet={'as_of':self.now,'candidate':c,'evidence_ids':['closed-candles']}
        self.reply={'assessment_id':'fixture','candidate_id':c['id'],'symbol':c['symbol'],'as_of':self.now,
                    'valid_until':self.now+20,'decision':'BUY','reason':'Fixture evaluation','invalidation_conditions':['Stop touched'],'evidence_ids':['closed-candles']}
        self.adapter=Mock();self.adapter.assess_evidence.return_value=(self.reply,{'input_tokens':100,'output_tokens':70,'total_tokens':170})
    def tearDown(self):self.store.close();self.tmp.cleanup()
    def test_bounded_exact_model_request_and_duplicate_cache(self):
        self.a.attach(self.adapter,'gpt-6-sol-account-fixture');r=self.a.assess(self.packet);self.assertEqual(r['decision'],'BUY')
        kw=self.adapter.assess_evidence.call_args.kwargs;self.assertEqual(kw['timeout'],8);self.assertEqual(kw['max_output_tokens'],350)
        self.a.assess(self.packet);self.adapter.assess_evidence.assert_called_once()
        self.assertEqual(self.a.economy.status()['calls_rolling_24h'],1)
    def test_outage_malformed_expired_identity_and_invented_evidence_mean_wait(self):
        for replacement in ({},dict(self.reply,valid_until=self.now-1),dict(self.reply,candidate_id='wrong'),dict(self.reply,evidence_ids=['invented'])):
            with self.assertRaises(Blocked):validate(replacement,self.packet,self.now)
        self.a.attach(self.adapter,'gpt-6-sol-account-fixture');self.adapter.assess_evidence.side_effect=TimeoutError()
        self.assertEqual(self.a.assess(self.packet)['decision'],'WAIT')
        self.assertEqual(self.a.economy.status()['failed_attempts'],1)
    def test_disabled_and_unavailable_models_never_call_provider(self):
        self.assertEqual(self.a.assess(self.packet)['decision'],'WAIT');self.adapter.assess_evidence.assert_not_called()
        with self.assertRaises(Blocked):self.a.attach(self.adapter,'another-model')
    def test_limits_do_not_retry_failed_candidates_or_exceed_budget(self):
        self.a.attach(self.adapter,'gpt-6-sol-account-fixture');self.a.economy.daily=0
        self.assertEqual(self.a.assess(self.packet)['decision'],'WAIT');self.adapter.assess_evidence.assert_not_called()


class IntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.store=ResearchStore(self.tmp.name)
        self.s=IntelligenceService(self.store,FakePublic([]),spot_lab=True);self.now=time.time()
        self.s.market_clock.update(self.now,self.now,self.now, time.monotonic(),time.monotonic())
        self.s.stream=SimpleNamespace(ready=threading.Event());self.s.stream.ready.set();self.s.paper_mode='RUNNING'
        self.s.prepared={('BTCUSDT',i):{'rows':[{'close_ms':int((self.now-1)*1000)}],'rules':parse_rules(metadata())} for i in ('1m','5m')}
        self.s.last_feed={'BTCUSDT':{'bid':100,'ask':100,'received_at':self.now}}
    def tearDown(self):self.s.close();self.store.close();self.tmp.cleanup()
    def candidate(self):return {**proposed('1m',self.now),'data_cutoff':self.s.prepared[('BTCUSDT','1m')]['rows'][-1]['close_ms']/1000,'indicators':{},'eligible':True,'strategy_version':'fixture','holding_minutes':30}
    def test_no_model_connection_only_executes_separate_baseline(self):
        with patch('aegis.intelligence.spot_lab.propose',return_value=[self.candidate()]):self.s.dispatch_pending()
        self.assertIsNone(self.s.spot.paper.state['position']);self.assertIsNotNone(self.s.spot.baseline.state['position'])
        self.assertEqual(self.s.spot.analyst.economy.status()['calls_rolling_24h'],0)
    def test_pause_invalidates_approval_but_never_blocks_tick_stop(self):
        c=self.candidate();self.s.spot.paper.enter(c,self.s.spot.book('BTCUSDT',self.s.market_now()),self.s.market_now())
        pending=Future();self.s.spot.future=pending;self.s.pause()
        self.s.on_quote('BTCUSDT',98,98.01,time.time())
        self.assertFalse(pending.done());self.assertIsNone(self.s.spot.paper.state['position'])
        pending.set_result(({},c,self.s.generation-1,{'decision':'BUY','reason':'Fixture','valid_until':self.now+20}))
        self.s.dispatch_pending();self.assertIsNone(self.s.spot.paper.state['position'])
    def test_restart_does_not_mix_old_or_baseline_ledgers(self):
        self.s.spot.baseline.enter(self.candidate(),self.s.spot.book('BTCUSDT',self.s.market_now()),self.s.market_now())
        again=IntelligenceService(self.store,FakePublic([]),spot_lab=True)
        try:
            self.assertEqual(again.paper_mode,'PAUSED');self.assertIsNone(again.spot.paper.state['position'])
            self.assertIsNotNone(again.spot.baseline.state['position']);self.assertEqual(again.paper.state['cash'],'10')
        finally:again.close()
    def test_new_mode_snapshots_only_its_ledger_and_fresh_quote_basis(self):
        self.s.prepared={};v=self.s.snapshot()
        self.assertEqual(v['paper_key'],'paper:spot-gpt-v1');self.assertFalse(v['feed']['BTCUSDT']['stale'])
        self.assertEqual(v['spot_lab']['model_improvement'].split(';')[0],'Unmeasured')
    def test_cached_start_loads_public_warmup_without_fitting_or_model_access(self):
        self.s.spot.save_frames();self.s.prepared={};self.s.restore()
        self.assertIn(('BTCUSDT','1m'),self.s.prepared);self.assertFalse(self.s.spot.analyst.economy.enabled)

    def test_restart_resumes_position_feed_without_rearming_or_model_dispatch(self):
        self.s.spot.baseline.enter(self.candidate(),self.s.spot.book('BTCUSDT',self.s.market_now()),self.s.market_now())
        del self.s.stream
        stream=SimpleNamespace(start=Mock());factory=Mock(return_value=stream)
        self.assertTrue(self.s.spot.resume_exits(factory));stream.start.assert_called_once()
        self.assertEqual(self.s.paper_mode,'PAUSED');self.assertEqual(self.s.spot.analyst.economy.status()['calls_rolling_24h'],0)

    def test_flat_restart_does_not_connect_or_spend_model_usage(self):
        factory=Mock();self.assertFalse(self.s.spot.resume_exits(factory));factory.assert_not_called()

    def test_transient_clock_refresh_failure_only_pauses_when_prior_calibration_expires(self):
        from aegis.intelligence.stream import PublicStream
        stream=PublicStream(self.s,['BTCUSDT']);stream.stop=Mock();stream.stop.wait.side_effect=[False,True]
        stream.sync_clock=Mock(side_effect=TimeoutError());self.s.last_error=None
        stream.clock_loop();self.assertEqual(self.s.paper_mode,'RUNNING')
        stream.stop.wait.side_effect=[False,True]
        with patch.object(self.s.market_clock,'status',return_value={'ready':False}):stream.clock_loop()
        self.assertEqual(self.s.paper_mode,'PAUSED');self.assertIn('Clock refresh failed',self.s.last_error)

    def test_valid_gpt_approval_is_rechecked_and_recorded_without_touching_other_ledger(self):
        c=self.candidate();self.s.spot.seen['BTCUSDT']=(self.s.prepared[('BTCUSDT','1m')]['rows'][-1]['close_ms'],self.s.prepared[('BTCUSDT','5m')]['rows'][-1]['close_ms'])
        f=Future();f.set_result(({},c,self.s.generation,{'decision':'BUY','reason':'Fixture approval','valid_until':self.now+20,'assessment_id':'fixture'}));self.s.spot.future=f
        self.s.dispatch_pending();self.assertIsNotNone(self.s.spot.paper.state['position'])
        self.assertIsNone(self.s.spot.baseline.state['position']);self.assertEqual(self.s.paper.state['cash'],'10')

    def test_approval_cannot_chase_a_price_or_survive_new_candle(self):
        c=self.candidate();f=Future();f.set_result(({},c,self.s.generation,{'decision':'BUY','reason':'Fixture','valid_until':self.now+20}));self.s.spot.future=f
        self.s.last_feed['BTCUSDT'].update(bid=103,ask=103)
        with patch('aegis.intelligence.spot_lab.propose',return_value=[]):self.s.dispatch_pending()
        self.assertIsNone(self.s.spot.paper.state['position'])


class ConnectionTests(unittest.TestCase):
    def setUp(self):
        from aegis.intelligence.connection import PlanConnection
        self.tmp=tempfile.TemporaryDirectory();self.store=ResearchStore(self.tmp.name)
        self.s=IntelligenceService(self.store,FakePublic([]),spot_lab=True);self.conn=PlanConnection(self.s,8778)
    def tearDown(self):self.s.close();self.store.close();self.tmp.cleanup()
    def test_connection_status_excludes_credentials_and_account_identity(self):
        self.conn.chat.status=Mock(return_value={'connected':True,'plan_enabled':True,'models':[],
            'accounts':[{'email':'fixture@example.invalid','access_token':'fixture-token'}],'active':'fixture-user'})
        status=self.conn.status();self.assertNotIn('accounts',status);self.assertNotIn('active',status)
        self.assertNotIn('fixture-token',str(status));self.assertNotIn('fixture@example.invalid',str(status))
    def test_saved_connection_copies_only_chat_profile_not_binance_credentials(self):
        data={'binance':{'secret':'fixture-binance-secret'},'host_id':'fixture-host','chatgpt':{'active':'p','profiles':{'p':{'access_token':'fixture-chat-token','client_id':'fixture-client','scopes':['chatgpt.tokens.use.direct'],'unexpected':'do not copy'}}}}
        source=Mock();source.read.return_value=data
        self.conn.catalog=Mock(return_value={'models':[]})
        with patch('aegis.intelligence.connection.Vault',return_value=source),patch('pathlib.Path.is_file',return_value=True):self.conn.saved()
        copied=self.conn.vault.read();self.assertNotIn('binance',copied);self.assertNotIn('unexpected',copied['chatgpt']['profiles']['p'])
        source.update.assert_not_called();self.assertEqual(copied['host_id'],'fixture-host')
    def test_selection_uses_exact_catalog_model_and_invalidates_prior_approvals(self):
        self.conn.chat.status=Mock(return_value={'plan_enabled':True});self.s.paper_mode='RUNNING';generation=self.s.generation
        with self.assertRaises(Blocked):self.conn.select('invented',known={'gpt-6-sol-fixture'})
        self.conn.select('gpt-6-sol-fixture',known={'gpt-6-sol-fixture'})
        self.assertEqual(self.s.spot.analyst.model,'gpt-6-sol-fixture');self.assertTrue(self.s.spot.analyst.economy.enabled)
        self.assertGreater(self.s.generation,generation);self.assertEqual(self.s.paper_mode,'PAUSED')
    def test_unavailable_selected_model_is_not_silently_replaced(self):
        self.store.put_state('spot_selected_model','gpt-6-sol-old-fixture')
        self.conn.chat.models=Mock(return_value=[{'slug':'gpt-6-sol-new-fixture'}])
        self.conn.catalog();self.assertFalse(self.s.spot.analyst.economy.enabled)
        self.assertIn('unavailable',self.conn.error)

    def test_dashboard_port_cannot_be_shared_by_another_workspace(self):
        from paper_server import server
        first=server(self.s,0)
        try:
            with self.assertRaises(OSError):server(self.s,first.server_address[1])
        finally:first.server_close();self.s.control.close()
