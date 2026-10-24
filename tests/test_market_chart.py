import copy
import tempfile
import unittest
from unittest.mock import Mock,patch
from aegis.intelligence.chart import charts
from aegis.intelligence.service import IntelligenceService
from aegis.intelligence.store import ResearchStore
from aegis.intelligence.stream import PublicStream
from aegis.intelligence.data import PublicDataError
from test_intelligence import bars,FakePublic


class MarketChartTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.store=ResearchStore(self.temp.name)
        self.service=IntelligenceService(self.store,FakePublic([]));self.rows=bars(100,'1m')
        self.service.prepared[('BTCUSDT','1m')]={'rows':copy.deepcopy(self.rows)}
        self.preview=bars(101,'1m')[-1]

    def tearDown(self):
        self.service.close();self.store.close();self.temp.cleanup()

    def event(self):
        b=self.preview
        return {'e':'kline','s':'BTCUSDT','k':{'i':'1m','x':False,'t':b['open_ms'],'T':b['close_ms'],
            'o':b['open'],'h':b['high'],'l':b['low'],'c':b['close'],'v':b['volume']}}

    def build(self,episodes=(),position=None):
        return charts(self.service.prepared,self.service.chart_previews,episodes,position,2000000000)[0]

    def episode(self,kind='ENTRY',**overrides):
        return {'id':kind,'symbol':'BTCUSDT','as_of':1800000000,'body':{'kind':kind,'wallet_key':'paper:forward-deterministic',
            'filled_qty':'.01','position':{'entry':'100'},'price':'103',**overrides}}

    def test_forming_preview_updates_chart_but_not_strategy_or_memory(self):
        PublicStream(self.service,['BTCUSDT']).handle(self.event())
        self.assertEqual(self.service.prepared[('BTCUSDT','1m')]['rows'],self.rows)
        self.assertEqual(self.build()['candles'][-1]['open_ms'],self.preview['open_ms'])
        self.assertFalse(self.build()['candles'][-1]['complete'])
        for table in ('predictions','decisions','memory_items','trade_episodes','model_calls'):
            self.assertEqual(self.store.db.execute('SELECT COUNT(*) FROM '+table).fetchone()[0],0)

    def test_only_filled_forward_paper_orders_get_buy_sell_markers(self):
        episodes=[self.episode(),self.episode('EXIT'),self.episode('WAIT'),self.episode(wallet_key='paper:historical-replay'),self.episode(filled_qty='0'),self.episode(filled_qty='NaN')]
        markers=self.build(episodes)['markers']
        self.assertEqual([m['side'] for m in markers],['BUY','SELL'])
        self.assertEqual([m['price'] for m in markers],['100','103'])
        self.assertTrue(all(m['mode']=='PAPER' for m in markers))

    def test_empty_execution_history_never_manufactures_markers(self):
        self.assertEqual(self.build()['markers'],[])

    def test_other_symbol_trades_never_appear_on_chart(self):
        e=self.episode();e['symbol']='ETHUSDT'
        self.assertEqual(self.build([e])['markers'],[])

    def test_bad_prices_do_not_get_markers(self):
        for price in ('NaN','Infinity','0','-1'):
            self.assertEqual(self.build([self.episode('EXIT',price=price)])['markers'],[])

    def test_accepted_candle_cannot_be_overwritten_by_unfinished_update(self):
        self.service.on_chart_candle('BTCUSDT','1m',{**self.rows[-1],'close':'100'},100)
        c=self.build()['candles'][-1]
        self.assertTrue(c['complete']);self.assertEqual(c['close'],self.rows[-1]['close'])

    def test_older_stream_update_cannot_replace_newer_preview(self):
        self.service.on_chart_candle('BTCUSDT','1m',self.preview,100,True)
        self.service.on_chart_candle('BTCUSDT','1m',self.preview,101,False)
        self.service.on_chart_candle('BTCUSDT','1m',self.rows[-1],102,False)
        self.assertTrue(self.service.chart_previews[('BTCUSDT','1m')]['exchange_closed'])
        self.assertEqual(self.build()['candles'][-1]['open_ms'],self.preview['open_ms'])

    def test_chart_is_bounded_and_does_not_invent_bars_in_gaps(self):
        rows=bars(240,'1m');rows.pop(-2)
        self.service.prepared[('BTCUSDT','1m')]['rows']=rows
        c=self.build()['candles'];self.assertEqual(len(c),200)
        self.assertEqual(c[-1]['open_ms']-c[-2]['open_ms'],120000)

    def test_position_levels_are_symbol_scoped_and_paper_labeled(self):
        position={'symbol':'BTCUSDT','entry':'100','stop_loss':'99','take_profit':'103'}
        self.assertEqual(self.build(position=position)['levels']['stop_loss'],'99')
        position['symbol']='ETHUSDT';self.assertIsNone(self.build(position=position)['levels'])

    def test_malformed_forming_ohlcv_is_rejected_before_chart_update(self):
        event=self.event();event['k']['h']='-1'
        with self.assertRaises(PublicDataError):PublicStream(self.service,['BTCUSDT']).handle(event)
        self.assertFalse(self.service.chart_previews)

    def test_invalid_completion_flag_is_rejected(self):
        event=self.event();event['k']['x']='false'
        with self.assertRaises(ConnectionError):PublicStream(self.service,['BTCUSDT']).handle(event)
        self.assertFalse(self.service.chart_previews)

    def test_rest_reconnect_retains_exchange_closed_bar_ahead_of_pc(self):
        b=self.preview;close=b['close_ms']/1000
        client=Mock()
        raw=[[r['open_ms'],r['open'],r['high'],r['low'],r['close'],r['volume'],r['close_ms']] for r in self.rows+[b]]
        client.get.side_effect=[{'serverTime':b['close_ms']+2000},raw]
        stream=PublicStream(self.service,['BTCUSDT'],['1m'],client)
        with patch('time.time',return_value=close-13):stream.resync()
        self.assertEqual(len(self.service.pending_candles),1)
        self.assertNotEqual(self.service.prepared[('BTCUSDT','1m')]['rows'][-1]['open_ms'],b['open_ms'])
        with patch('time.time',return_value=close+1):self.service.flush_candles(close+1)
        self.assertEqual(self.service.prepared[('BTCUSDT','1m')]['rows'][-1]['open_ms'],b['open_ms'])
        self.assertEqual(client.get.call_args_list[0].args,('time',))

    def test_rest_time_proof_cannot_promote_current_unfinished_snapshot(self):
        b=self.preview;close=b['close_ms']/1000;client=Mock()
        raw=[[r['open_ms'],r['open'],r['high'],r['low'],r['close'],r['volume'],r['close_ms']] for r in self.rows+[b]]
        client.get.side_effect=[{'serverTime':b['close_ms']-1000},raw]
        with patch('time.time',return_value=close-13):PublicStream(self.service,['BTCUSDT'],['1m'],client).resync()
        self.assertFalse(self.service.pending_candles)
        self.assertNotEqual(self.service.prepared[('BTCUSDT','1m')]['rows'][-1]['open_ms'],b['open_ms'])
