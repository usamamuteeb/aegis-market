import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock
from aegis.intelligence.control import ConsoleControl
from aegis.intelligence.store import ResearchStore


class ConsoleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = ResearchStore(self.temp.name)
        self.service = SimpleNamespace(store=self.store, stop=threading.Event(), job={'running': False},
            prepared={('BTCUSDT','1m'): {}}, last_feed={}, paper_mode='PAUSED', pause=Mock(), research=Mock())
        def pause(): self.service.paper_mode='PAUSED'
        self.service.pause.side_effect=pause
        self.stream=SimpleNamespace(start=Mock(),ready=threading.Event())
        self.factory=Mock(return_value=self.stream)
        self.control=ConsoleControl(self.service,self.factory,feed_timeout=.15)

    def tearDown(self):
        self.service.stop.set();self.control.close();self.store.close();self.temp.cleanup()

    def join(self):
        self.control.thread.join(2)
        self.assertFalse(self.control.thread.is_alive())

    def quote(self):
        self.stream.ready.set()
        self.service.last_feed={'BTCUSDT':{'received_at':time.time()}}

    def test_one_start_connects_and_observes_without_duplicate_threads(self):
        self.quote();self.control.start();self.join()
        self.assertEqual(self.service.paper_mode,'RUNNING')
        self.control.start();self.factory.assert_called_once();self.stream.start.assert_called_once()
        self.service.research.assert_not_called()

    def test_live_check_never_starts_feed_research_or_paper(self):
        self.control.start('live')
        self.assertEqual(self.service.paper_mode,'PAUSED');self.factory.assert_not_called()
        self.service.research.assert_not_called();self.assertEqual(self.control.stage,'LIVE_BLOCKED')

    def test_missing_research_is_prepared_automatically(self):
        self.service.prepared={}
        def research(*args):
            self.service.prepared={('BTCUSDT','1m'): {}};self.quote()
        self.service.research.side_effect=research
        self.control.start();self.join()
        self.service.research.assert_called_once();self.assertEqual(self.service.paper_mode,'RUNNING')

    def test_stale_feed_cannot_arm_and_has_readable_error(self):
        self.stream.ready.set();self.service.last_feed={'BTCUSDT':{'received_at':time.time()-10}}
        self.control.start();self.join()
        self.assertEqual(self.service.paper_mode,'PAUSED');self.assertEqual(self.control.stage,'BLOCKED')
        self.assertIn('Fresh prices',self.control.message)

    def test_stop_during_start_prevents_late_rearm(self):
        self.control.start();self.control.pause(emergency=True);self.quote();self.join()
        self.assertEqual(self.service.paper_mode,'PAUSED');self.assertEqual(self.control.stage,'STOPPED')

    def test_fresh_cached_quote_cannot_arm_before_resynchronization(self):
        self.service.last_feed={'BTCUSDT':{'received_at':time.time()}}
        self.control.start();self.join()
        self.assertEqual(self.service.paper_mode,'PAUSED')

    def test_all_markets_must_have_fresh_prices(self):
        self.service.prepared[('ETHUSDT','1m')]={};self.quote()
        self.control.start();self.join();self.assertEqual(self.service.paper_mode,'PAUSED')

    def test_logs_persist_but_restart_never_rearms(self):
        self.control.pause();again=ConsoleControl(self.service,self.factory)
        self.assertTrue(any(e['stage']=='PAUSED' for e in again.events))
        self.assertEqual(self.service.paper_mode,'PAUSED');self.factory.assert_not_called()

    def test_stream_failure_does_not_silently_rearm(self):
        self.quote();self.control.start();self.join();self.service.pause();self.stream.ready.clear()
        self.assertEqual(self.control.snapshot()['stage'],'RECOVERY')
        self.quote();self.assertEqual(self.service.paper_mode,'PAUSED')

    def test_invalid_mode_rejected(self):
        with self.assertRaises(ValueError): self.control.start('automatic-live')

    def test_recovered_feed_tells_user_to_resume_without_arming(self):
        self.control.stage='OBSERVING';self.service.stream=self.stream;self.quote()
        result=self.control.snapshot()
        self.assertEqual(result['stage'],'RECOVERED');self.assertIn('press Start agent',result['message'])
        self.assertEqual(self.service.paper_mode,'PAUSED')
        self.factory.assert_not_called()

    def test_restart_requests_resync_and_requires_quotes_after_the_new_connection(self):
        self.service.stream=self.stream;self.stream.thread=SimpleNamespace(is_alive=lambda:True)
        self.quote()
        def restart():
            self.assertFalse(self.stream.ready.is_set());self.assertTrue(self.service.needs_resync)
            self.stream.connected_at=time.time();self.quote()
        self.stream.start.side_effect=restart
        self.control.start();self.join();self.assertEqual(self.service.paper_mode,'RUNNING')

    def test_recent_quote_from_before_connection_cannot_rearm(self):
        self.quote();self.service.stream=self.stream;self.stream.connected_at=time.time()+.01
        self.assertFalse(self.control.fresh())

    def test_spot_system_events_use_the_same_exchange_clock_as_decisions(self):
        self.service.spot=object();self.service.market_clock=SimpleNamespace(sample={'fixture':True})
        self.service.market_now=lambda:1800000013
        self.control._event('PAUSED','Clock fixture')
        self.assertEqual(self.control.events[-1]['at'],1800000013)
        self.assertEqual(self.control.events[-1]['time_basis'],'exchange estimate')
