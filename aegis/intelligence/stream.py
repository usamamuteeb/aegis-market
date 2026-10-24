"""Small TLS WebSocket public feed; bounded frames, reconnects and REST resync.

No subscription/account tokens, no private endpoints, no mutation retries.
"""
import base64
import hashlib
import json
import os
import socket
import ssl
import struct
import threading
import time
from .data import parse_bar,PublicClient,INTERVALS

def read_exact(sock,n):
    chunks=[]
    while n:
        b=sock.recv(n)
        if not b:raise ConnectionError('Public stream disconnected')
        chunks.append(b);n-=len(b)
    return b''.join(chunks)

def read_frame(sock):
    first,second=read_exact(sock,2);fin=bool(first&128);opcode=first&15
    if first&112 or second&128:raise ConnectionError('Invalid server WebSocket frame')
    length=second&127
    if length==126:length=struct.unpack('!H',read_exact(sock,2))[0]
    if length==127:length=struct.unpack('!Q',read_exact(sock,8))[0]
    if length>1048576 or opcode>=8 and (not fin or length>125):raise ConnectionError('Oversized public stream frame')
    return fin,opcode,read_exact(sock,length)

def send_frame(sock,opcode,payload=b''):
    mask=os.urandom(4);length=len(payload)
    header=bytes([128|opcode,128|length]) if length<126 else bytes([128|opcode,128|126])+struct.pack('!H',length)
    sock.sendall(header+mask+bytes(x^mask[i%4] for i,x in enumerate(payload)))

class PublicStream:
    def __init__(self,service,symbols,intervals=('1m','5m'),client=None):
        self.service=service;self.symbols=symbols;self.intervals=intervals;self.client=client or PublicClient()
        self.stop=threading.Event();self.socket=None;self.sequences={};self.thread=None;self.failures=0
        self.ready=threading.Event()
        self.clock_thread=None
        self.connected_at=0
        self.refresh=threading.Event()
        self.state_lock=threading.RLock()
        self.connection={'phase':'IDLE','failures':0,'reconnections':0,'last_failure':None,'retry_at':None}

    def status(self):
        with self.state_lock:return dict(self.connection)

    def state(self,phase,**details):
        with self.state_lock:self.connection.update(phase=phase,**details)

    def request_resync(self):
        """Wake the reader for a requested refresh, without recording an outage."""
        self.ready.clear();self.service.pause();self.refresh.set()
        if self.socket:
            try:self.socket.shutdown(socket.SHUT_RDWR)
            except OSError:pass

    def failure(self,exc,phase):
        # Only public protocol/network diagnostics; never expose arbitrary exception
        # text (which might come from another subsystem) or request headers.
        if isinstance(exc,socket.gaierror):reason='DNS lookup failed'
        elif isinstance(exc,(TimeoutError,socket.timeout)):reason='Public endpoint timed out'
        elif isinstance(exc,ssl.SSLError):reason='TLS connection failed'
        elif isinstance(exc,ConnectionError):
            reason=str(exc) if str(exc) in ('Public stream disconnected','Public stream closing','Public stream handshake rejected','Completed candle recovery required','PC and exchange clocks differ by over 30 seconds','Resync still contains gaps') else 'Public stream protocol error'
        elif isinstance(exc,ValueError):reason='Clock or public message validation failed'
        else:reason='Public data or local processing failed'
        failure={'at':self.service.market_now() if hasattr(self.service,'market_now') else time.time(),
                 'phase':phase,'reason':reason,'type':type(exc).__name__}
        self.state('RETRYING',failures=self.failures,last_failure=failure,retry_at=time.time()+min(60,2**min(self.failures,6)))
        store=getattr(self.service,'store',None)
        if store is not None:
            store.append('observations',failure,as_of=failure['at'],version='public-feed-failure-v1')

    def sync_clock(self):
        wall=time.time();mono=time.monotonic();server=int(self.client.get('time')['serverTime'])/1000
        if getattr(self.service,'spot',None):self.service.market_clock.update(server,wall,time.time(),mono,time.monotonic())
        return server

    def clock_loop(self):
        while not self.stop.wait(60):
            try:
                self.sync_clock()
                if (self.service.last_error or '').startswith('Clock refresh failed'):self.service.last_error=None
            except Exception:
                # A slow/failed refresh must not replace a still-valid estimate.
                # Quote handling independently pauses when the last sample expires.
                if not self.service.market_clock.status()['ready']:
                    self.service.pause();self.service.last_error='Clock refresh failed; new entries paused until recovery and Start'

    def open(self):
        streams='/'.join(s.lower()+'@'+suffix for s in self.symbols for suffix in ['bookTicker']+['kline_'+i for i in self.intervals])
        sock=ssl.create_default_context().wrap_socket(socket.create_connection(('data-stream.binance.vision',443),timeout=12),server_hostname='data-stream.binance.vision')
        key=base64.b64encode(os.urandom(16)).decode()
        request=f'GET /stream?streams={streams} HTTP/1.1\r\nHost: data-stream.binance.vision\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n'
        sock.sendall(request.encode());header=b''
        while not header.endswith(b'\r\n\r\n'):
            header+=read_exact(sock,1)
            if len(header)>16384:raise ConnectionError('Oversized stream handshake')
        lines=header.decode('ascii').split('\r\n');headers=dict(x.split(':',1) for x in lines[1:] if ':' in x)
        headers={k.lower():v.strip() for k,v in headers.items()}
        expected=base64.b64encode(hashlib.sha1((key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()).decode()
        if ' 101 ' not in lines[0] or headers.get('sec-websocket-accept')!=expected:sock.close();raise ConnectionError('Public stream handshake rejected')
        sock.settimeout(25);self.socket=sock

    def handle(self,event,received_at=None,monotonic_received=None):
        now=time.time() if received_at is None else received_at;data=event.get('data',event);symbol=data.get('s')
        if symbol not in self.symbols:return
        if data.get('e')=='kline':
            k=data['k'];interval=k['i']
            if interval not in self.intervals:return
            if not isinstance(k.get('x'),bool):raise ConnectionError('Invalid candle completion flag')
            if k['x']:
                event_at=int(data.get('E',0))/1000
                if event_at<=k['T']/1000:raise ConnectionError('Closed candle lacks a valid exchange completion timestamp')
            # Use the same numeric/schema validation for the current chart candle,
            # explicitly bypassing its completion time only in the display path.
            display_bar=parse_bar([k['t'],k['o'],k['h'],k['l'],k['c'],k['v'],k['T']],interval,(int(k['T'])+2)/1000)
            if display_bar:self.service.on_chart_candle(symbol,interval,display_bar,now,bool(k['x']))
            if not k['x']:return
            # Validate against the exchange's closure proof. Service defers any bar
            # still ahead of local time instead of silently losing the only close.
            bar=parse_bar([k['t'],k['o'],k['h'],k['l'],k['c'],k['v'],k['T']],interval,max(now,event_at))
            if bar:self.service.on_candle(symbol,interval,bar)
        elif 'u' in data and 'b' in data and 'a' in data:
            seq=int(data['u'])
            if seq<=self.sequences.get(symbol,-1):return
            self.sequences[symbol]=seq
            self.service.on_quote(symbol,data['b'],data['a'],now,source_event_at=data.get('E',0)/1000 or None,monotonic_received=monotonic_received)

    def resync(self):
        self.service.pause();self.service.paper_mode='PAUSED'
        # Sample exchange time BEFORE fetching candles. A subsequent REST row is
        # complete only if its close predates that proof; never promote an earlier
        # unfinished snapshot using a timestamp obtained after the snapshot.
        exchange_cutoff=self.sync_clock()
        if abs(exchange_cutoff-time.time())>30:raise ConnectionError('PC and exchange clocks differ by over 30 seconds')
        for symbol in self.symbols:
            for interval in self.intervals:
                prepared=self.service.prepared.get((symbol,interval))
                if not prepared:continue
                rows=self.client.get('klines',symbol=symbol,interval=interval,limit=150)
                verified=[bar for row in rows if (bar:=parse_bar(row,interval,exchange_cutoff)) is not None]
                if any(b['open_ms']-a['open_ms']!=INTERVALS[interval]*1000 for a,b in zip(verified,verified[1:])):raise ConnectionError('Resync still contains gaps')
                local_cutoff=self.service.market_now() if hasattr(self.service,'market_now') else time.time()
                completed=[bar for bar in verified if bar['close_ms']/1000<local_cutoff]
                prepared['rows']=completed
                for bar in verified:
                    if bar['close_ms']/1000>=local_cutoff:self.service.on_candle(symbol,interval,bar)
        self.sequences.clear()
        self.service.needs_resync=False
        if getattr(self.service,'spot',None):self.service.spot.save_frames()

    def run(self):
        while not self.stop.is_set():
            intentional=False;phase='RESYNC'
            try:
                self.refresh.clear();self.ready.clear();self.state('RESYNC',retry_at=None);self.resync()
                phase='CONNECTING';self.state(phase);self.open();self.connected_at=time.time();self.ready.set();self.failures=0;fragment=b'';messages=0
                phase='STREAM';self.state('CONNECTED',failures=0,connected_at=self.connected_at,retry_at=None)
                self.service.last_error=None
                while not self.stop.is_set():
                    fin,opcode,payload=read_frame(self.socket)
                    if opcode==8:raise ConnectionError('Public stream closing')
                    if opcode==9:send_frame(self.socket,10,payload);continue
                    if opcode==10:continue
                    if opcode not in (0,1):raise ConnectionError('Unsupported public stream frame')
                    fragment+=payload
                    if len(fragment)>1048576:raise ConnectionError('Public stream message too large')
                    if not fin:continue
                    event=json.loads(fragment);fragment=b'';received=time.time();mono=time.monotonic();self.handle(event,received,mono);messages+=1
                    if self.service.needs_resync:raise ConnectionError('Completed candle recovery required')
            except Exception as exc:
                self.ready.clear()
                intentional=self.refresh.is_set() or self.stop.is_set()
                self.service.pause();self.service.paper_mode='PAUSED'
                if not intentional:
                    self.failures+=1;self.failure(exc,phase)
                    self.service.last_error=self.status()['last_failure']['reason']+'; entries paused. Reconnecting automatically.'
            finally:
                if self.socket:
                    try:self.socket.close()
                    except OSError:pass
                    self.socket=None
            if intentional:continue
            if not self.stop.is_set():
                with self.state_lock:self.connection['reconnections']+=1
            self.stop.wait(min(60,2**min(self.failures,6)))
        self.state('STOPPED',retry_at=None)

    def start(self):
        if self.thread and self.thread.is_alive():return
        self.thread=threading.Thread(target=self.run,daemon=True,name='public-paper-feed');self.thread.start()
        if getattr(self.service,'spot',None) and not self.clock_thread:
            self.clock_thread=threading.Thread(target=self.clock_loop,daemon=True,name='public-clock-refresh');self.clock_thread.start()
    def close(self):
        self.ready.clear()
        self.stop.set()
        if self.socket:
            try:self.socket.shutdown(socket.SHUT_RDWR)
            except OSError:pass
        if self.thread:self.thread.join(timeout=3)
        if self.clock_thread:self.clock_thread.join(timeout=1)
