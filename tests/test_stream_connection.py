import socket
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock,patch
from aegis.intelligence.stream import PublicStream


class ConnectionRecoveryTests(unittest.TestCase):
    def service(self):
        s=SimpleNamespace(paper_mode='RUNNING',last_error=None,needs_resync=False,
                          pause=Mock(),market_now=time.time,store=Mock())
        s.pause.side_effect=lambda:setattr(s,'paper_mode','PAUSED')
        return s

    def test_manual_refresh_wakes_reader_without_an_outage_or_backoff(self):
        s=self.service();stream=PublicStream(s,['BTCUSDT'])
        stream.resync=Mock();stream.socket=Mock();opened=0
        def open_socket():
            nonlocal opened
            opened+=1
            stream.socket=Mock()
        def read(sock):
            if opened==1:stream.request_resync();raise ConnectionError('Public stream disconnected')
            stream.stop.set();raise ConnectionError('Public stream disconnected')
        stream.open=open_socket
        with patch('aegis.intelligence.stream.read_frame',side_effect=read),patch.object(stream.stop,'wait',wraps=stream.stop.wait) as wait:
            stream.run();wait.assert_not_called()
        self.assertEqual(opened,2);self.assertEqual(stream.status()['reconnections'],0)
        self.assertIsNone(stream.status()['last_failure']);self.assertIsNone(s.last_error)
        self.assertEqual(s.paper_mode,'PAUSED');s.store.append.assert_not_called()

    def test_real_timeout_is_recorded_then_recovery_leaves_entries_paused(self):
        s=self.service();stream=PublicStream(s,['BTCUSDT']);stream.resync=Mock();opened=0
        def open_socket():
            nonlocal opened
            opened+=1
            if opened==1:raise TimeoutError('private text must not reach UI')
            stream.socket=Mock()
        def read(sock):stream.stop.set();raise ConnectionError('Public stream disconnected')
        stream.open=open_socket
        with patch('aegis.intelligence.stream.read_frame',side_effect=read),patch.object(stream.stop,'wait',return_value=False) as wait:
            stream.run();wait.assert_called_once_with(2)
        failure=stream.status()['last_failure']
        self.assertEqual(failure['reason'],'Public endpoint timed out');self.assertEqual(failure['phase'],'CONNECTING')
        self.assertEqual(s.paper_mode,'PAUSED');self.assertIsNone(s.last_error)
        self.assertEqual(s.store.append.call_count,1);self.assertNotIn('private text',str(stream.status()))

    def test_refresh_never_rearms_and_status_cannot_be_mutated_externally(self):
        s=self.service();stream=PublicStream(s,['BTCUSDT']);stream.ready.set();stream.socket=Mock()
        stream.request_resync()
        self.assertFalse(stream.ready.is_set());self.assertTrue(stream.refresh.is_set())
        self.assertEqual(s.paper_mode,'PAUSED');stream.socket.shutdown.assert_called_once_with(socket.SHUT_RDWR)
        snapshot=stream.status();snapshot['phase']='FORGED';self.assertEqual(stream.status()['phase'],'IDLE')

