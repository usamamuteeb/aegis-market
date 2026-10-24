import copy
import json
import tempfile
import time
import unittest
import re
import threading
from urllib.request import Request,urlopen
from urllib.error import HTTPError
from pathlib import Path
from unittest.mock import patch,Mock
from aegis.connectivity import ConnectionGuard
from aegis.qualification import require_live_release,status
from aegis.config import DEFAULTS
from aegis.domain import D,Blocked
from aegis.engine import Engine
from aegis.storage import Store
from aegis.research import replay,fetch_history
from test_system import FakeAPI,book,account


class ConnectionTests(unittest.TestCase):
    def test_recovery_needs_two_good_reads_and_never_claims_to_rearm(self):
        g=ConnectionGuard()
        self.assertFalse(g.observe(True,1));self.assertEqual(g.status,'RECOVERING')
        g.observe(True,2);self.assertEqual(g.status,'ONLINE')
        self.assertTrue(g.observe(False,3));self.assertEqual(g.status,'UNAVAILABLE')
        g.observe(True,4);self.assertEqual(g.status,'RECOVERING')
        g.observe(True,5);self.assertEqual(g.status,'ONLINE')
        self.assertIn('does not rearm',g.snapshot()['resume_policy'])

    def test_failure_backoff_is_bounded(self):
        g=ConnectionGuard();delays=[]
        for i in range(10):g.observe(False,100);delays.append(g.next_retry_at-100)
        self.assertEqual(delays[:4],[3,6,12,24]);self.assertTrue(all(d<=30 for d in delays))

    def test_worker_loss_invalidates_proposal_and_preserves_existing_protection(self):
        with tempfile.TemporaryDirectory() as temp:
            e=Engine(Path(temp),api=FakeAPI(),chat=Mock());e.state['position']={'stop_loss':'99','protection_order_list_id':9}
            e.cycle=Mock();count=[0]
            class Stop:
                def is_set(self):return count[0]>=4
                def wait(self,_):count[0]+=1
                def set(self):count[0]=4
            e.shutdown=Stop()
            def feed(_):
                if count[0]==1:e.state['mode']='RUNNING'
                if count[0]==2:return {},{'BTCUSDT':'outage','ETHUSDT':'outage','SOLUSDT':'outage'}
                return {sym:{**book(),'symbol':sym,'received_at':time.time()} for sym in DEFAULTS['symbols']},{}
            e.feed=Mock(fetch_all=feed)
            try:
                generation=e.state['generation'];e.run()
                self.assertEqual(e.state['mode'],'PAUSED');self.assertGreater(e.state['generation'],generation)
                self.assertEqual(e.state['position']['protection_order_list_id'],9)
                self.assertEqual(e.state['position']['stop_loss'],'99')
            finally:e.pool.shutdown(wait=False);e.research_pool.shutdown(wait=False);e.store.db.close()


class ReleaseTests(unittest.TestCase):
    def test_worker_health_has_no_secrets_and_shutdown_refuses_managed_exposure(self):
        from main import server
        with tempfile.TemporaryDirectory() as temp:
            e=Engine(Path(temp));probe=server(e,0);port=probe.server_address[1];probe.server_close()
            http=server(e,port);thread=threading.Thread(target=http.serve_forever,daemon=True);thread.start()
            base=f'http://127.0.0.1:{port}'
            try:
                with urlopen(base+'/health') as response:health=json.load(response)
                self.assertEqual(set(health),{'app','instance'});self.assertEqual(health['app'],'aegis')
                with urlopen(base) as response:token=re.search(r'name="aegis-session" content="([^"]+)"',response.read().decode())[1]
                e.state['position']={'symbol':'BTCUSDT','protection_order_list_id':9}
                request=lambda:Request(base+'/api/shutdown',data=b'{}',headers={'X-Aegis-Token':token,'Content-Type':'application/json'})
                with self.assertRaises(HTTPError) as denied:urlopen(request())
                self.assertEqual(denied.exception.code,400);denied.exception.close();self.assertFalse(e.shutdown.is_set())
                e.state['position']=None
                with urlopen(request()) as response:self.assertEqual(response.status,200)
                self.assertTrue(e.shutdown.is_set())
            finally:http.shutdown();http.server_close();thread.join();e.close();e.store.db.close()
    def test_no_report_and_edited_report_cannot_unlock_real_entries(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'report.json'
            with self.assertRaises(Blocked):require_live_release('live',path)
            path.write_text('{"live_eligible":true}',encoding='utf-8')
            with self.assertRaises(Blocked):require_live_release('live',path)
            require_live_release('testnet',path)

    def test_live_arm_is_blocked_before_exchange_or_model_calls(self):
        with tempfile.TemporaryDirectory() as temp:
            api=FakeAPI();api.status=lambda:{'configured':True,'environment':'live'}
            e=Engine(Path(temp),api=api,chat=Mock())
            try:
                with self.assertRaisesRegex(Blocked,'Live entries locked'):e.arm('ARM LIVE')
                self.assertFalse(api.calls);e.chat.models.assert_not_called()
            finally:e.pool.shutdown(wait=False);e.research_pool.shutdown(wait=False);e.store.db.close()


class HistoricalTests(unittest.TestCase):
    def dataset(self):
        return [{'open_time':i*300,'close_time':i*300+299.999,'open':100,'high':104,'low':98,'close':100,
                 'volume':100,'quote_volume':10000} for i in range(80)]
    def test_prefix_only_signals_next_bar_execution_and_stop_first(self):
        rows=self.dataset();calls=[]
        def evaluate(symbol,prefix,ask,tick,strategy,now):
            calls.append((prefix[-1]['close_time'],now))
            return {'eligible':len(calls)==1,'id':'one','symbol':symbol,'entry':'100','stop_loss':'99','take_profit':'103',
                    'created_at':now,'expires_at':now+90}
        with patch('aegis.research.evaluate',side_effect=evaluate),patch('aegis.research.plan',return_value={'qty':'.06'}):
            r=replay({'BTCUSDT':rows},{'BTCUSDT':book()['rules']},{**DEFAULTS,'allocation':10,'symbols':['BTCUSDT']},65*300,80*300,'.001')
        self.assertTrue(all(close<now for close,now in calls))
        self.assertEqual(r['metrics']['trades'],1)
        self.assertIn('stop',r['closed_trades'][0]['reason'])
        self.assertLess(D(r['metrics']['net_pnl']),0)
        self.assertEqual(D(r['ending_research_capital'])-10,D(r['metrics']['net_pnl']))
        self.assertEqual(r['closed_trades'][0]['opened_at'],65*300)

    def test_empty_or_nonadvancing_history_is_rejected(self):
        with self.assertRaises(Blocked):fetch_history('BTCUSDT',300,900,lambda *a,**k:[])
        row=[0,'100','101','99','100','1',299999,'100']
        with self.assertRaises(Blocked):fetch_history('BTCUSDT',300,900,lambda *a,**k:[row])

    def test_no_candidate_cannot_be_described_as_a_high_win_rate(self):
        with patch('aegis.research.evaluate',return_value={'eligible':False}):
            r=replay({'BTCUSDT':self.dataset()},{'BTCUSDT':book()['rules']},{**DEFAULTS,'allocation':10,'symbols':['BTCUSDT']},65*300,80*300,'.001')
        self.assertEqual(r['metrics']['trades'],0);self.assertIsNone(r['metrics']['win_rate_pct'])
        self.assertIsNone(r['metrics']['profit_factor']);self.assertEqual(D(r['ending_research_capital']),10)

if __name__=='__main__':unittest.main()
