import tempfile
import time
import unittest
from unittest.mock import Mock,patch
from aegis.intelligence.store import ResearchStore
from aegis.intelligence.service import IntelligenceService
from aegis.intelligence.stream import PublicStream
from test_intelligence import bars,FakePublic


class FeedClockRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.store=ResearchStore(self.temp.name)
        self.service=IntelligenceService(self.store,FakePublic([]))
        self.rows=bars(100,'1m');self.bar=bars(101,'1m')[-1]
        self.closed=self.bar['close_ms']/1000
        self.service.prepared[('BTCUSDT','1m')]={'rows':self.rows}

    def tearDown(self):
        self.service.close();self.store.close();self.temp.cleanup()

    def event(self,bar):
        return {'e':'kline','s':'BTCUSDT','E':bar['close_ms']+2,'k':{'i':'1m','x':True,'t':bar['open_ms'],
            'T':bar['close_ms'],'o':bar['open'],'h':bar['high'],'l':bar['low'],'c':bar['close'],'v':bar['volume']}}

    def test_slow_pc_retains_closed_event_until_local_time_passes(self):
        stream=PublicStream(self.service,['BTCUSDT'])
        with patch('time.time',return_value=self.closed-13):stream.handle(self.event(self.bar))
        self.assertEqual(len(self.service.pending_candles),1)
        self.assertEqual(self.service.prepared[('BTCUSDT','1m')]['rows'][-1],self.rows[-1])
        with patch('time.time',return_value=self.closed+1):
            self.service.flush_candles(self.closed+1)
        self.assertEqual(self.service.prepared[('BTCUSDT','1m')]['rows'][-1],self.bar)
        self.assertEqual(len(self.service.pending_candles),0)
        self.assertEqual(len(self.store.records('observations')),1)

    def test_unfinished_candle_never_enters_queue(self):
        stream=PublicStream(self.service,['BTCUSDT']);event=self.event(self.bar);event['k']['x']=False
        with patch('time.time',return_value=self.closed-13):stream.handle(event)
        self.assertFalse(self.service.pending_candles)

    def test_false_completion_proof_is_rejected(self):
        event=self.event(self.bar);event['E']=self.bar['close_ms']
        with self.assertRaises(ConnectionError):PublicStream(self.service,['BTCUSDT']).handle(event)

    def test_large_clock_error_blocks_instead_of_consuming_future(self):
        with patch('time.time',return_value=self.closed-90):self.service.on_candle('BTCUSDT','1m',self.bar)
        self.assertFalse(self.service.pending_candles);self.assertTrue(self.service.needs_resync)
        self.assertIn('clock',self.service.last_error)

    def test_gap_requests_actual_rest_recovery_and_pauses(self):
        gap=bars(102,'1m')[-1]
        with patch('time.time',return_value=gap['close_ms']/1000+1):self.service.on_candle('BTCUSDT','1m',gap)
        self.assertTrue(self.service.needs_resync);self.assertEqual(self.service.paper_mode,'PAUSED')

    def test_duplicate_closed_event_is_persisted_once(self):
        with patch('time.time',return_value=self.closed+1):
            self.service.on_candle('BTCUSDT','1m',self.bar);self.service.on_candle('BTCUSDT','1m',self.bar)
        self.assertEqual(len(self.store.records('observations')),1)

    def test_flat_wallet_has_real_observation_marks_without_trades(self):
        self.service.prepared={}
        start=time.time()
        for offset in (0,1,6):
            with patch('time.time',return_value=start+offset):self.service.on_quote('BTCUSDT','100','100.01',start+offset)
        self.assertEqual(len(self.service.paper.state['equity']),2)
        self.assertEqual(self.service.paper.metrics()['marked_equity'],'10')
        self.assertEqual(self.store.records('trade_episodes'),[])
        self.assertEqual(self.service.analyst.economy.status()['calls_rolling_24h'],0)

    def test_signal_health_exposes_stale_candles_with_fresh_quotes(self):
        with patch('time.time',return_value=self.rows[-1]['close_ms']/1000+180):
            h=self.service.signal_health()[0]
        self.assertFalse(h['fresh']);self.assertEqual(h['age_seconds'],180)
