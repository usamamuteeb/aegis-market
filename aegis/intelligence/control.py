"""One-button public-paper startup. No live adapter or model activation."""
import threading
import time
from .stream import PublicStream

LIVE_BLOCKERS = [
    'Live execution integration is incomplete.',
    'Entry-to-exchange-protection gap is unresolved.',
    'Exchange lifecycle and disconnect acceptance are not verified.',
    'Independent net profitability and GPT comparison are not established.',
]


class ConsoleControl:
    def __init__(self, service, stream_factory=PublicStream, feed_timeout=40):
        self.service = service
        self.stream_factory = stream_factory
        self.feed_timeout = feed_timeout
        self.lock = threading.RLock()
        self.cancel = threading.Event()
        self.thread = None
        self.stage = 'IDLE'
        self.message = 'Press Start to prepare research, connect prices and begin paper observation.'
        self.events = service.store.state('console_activity', [])
        self._event('READY', 'Dashboard ready. Entries are paused; real exchange orders are disabled.')

    def _event(self, stage, message):
        with self.lock:
            self.stage, self.message = stage, message
            clock=getattr(self.service,'market_clock',None)
            event_at=self.service.market_now() if getattr(self.service,'spot',None) else time.time()
            basis='exchange estimate' if clock and clock.sample else 'PC clock'
            self.events = (self.events + [{'at': event_at, 'stage': stage, 'message': message,'time_basis':basis}])[-200:]
            self.service.store.put_state('console_activity', self.events)

    def start(self, mode='paper'):
        if mode not in ('paper', 'live'):
            raise ValueError('Choose paper or live')
        with self.lock:
            if mode == 'live':
                # A readiness request must never instantiate private clients or arm entries.
                self._event('LIVE_BLOCKED', 'Real trading blocked: ' + ' '.join(LIVE_BLOCKERS))
                return
            if self.thread and self.thread.is_alive() or self.service.paper_mode == 'RUNNING':
                return
            self.cancel.clear()
            self._event('PREPARING', 'Starting automatic public-data checks. No account access or model calls.')
            self.thread = threading.Thread(target=self._prepare, daemon=True, name='paper-startup')
            self.thread.start()

    def _wait(self, seconds=.1):
        return self.cancel.wait(seconds) or self.service.stop.is_set()

    def _progress(self, stage, message):
        with self.lock:
            if self.cancel.is_set() or self.service.stop.is_set(): return False
            self._event(stage, message)
            return True

    def fresh(self):
        symbols = {s for s, _ in self.service.prepared}
        now = time.time()
        stream = getattr(self.service, 'stream', None)
        health=self.service.signal_health() if hasattr(self.service,'signal_health') else None
        candles_ok=health is None or bool(health) and all(x['fresh'] for x in health)
        clock_ok=not getattr(self.service,'spot',None) or self.service.market_clock.status()['ready']
        return clock_ok and candles_ok and bool(stream and stream.ready.is_set() and symbols) and all(s in self.service.last_feed and getattr(stream,'connected_at',0)<=self.service.last_feed[s]['received_at']<=now and now-self.service.last_feed[s]['received_at']<=3 for s in symbols)

    def _prepare(self):
        try:
            while self.service.job['running']:
                if self._wait(): return
            if self.cancel.is_set() or self.service.stop.is_set(): return
            if getattr(self.service,'spot',None):
                if not self._progress('PREPARING','Checking four Spot markets; reusing saved research. No model request during startup.'):return
                self.service.spot.prepare_markets(self.cancel)
            elif not self.service.prepared:
                if not self._progress('RESEARCH', 'Downloading and evaluating public BTCUSDT history. This may take several minutes.'): return
                self.service.research(('BTCUSDT',), ('1m', '5m'), 14)
            if self.cancel.is_set() or self.service.stop.is_set(): return
            if not self.service.prepared:
                raise ValueError('Research is incomplete. Check the connection and press Start to resume.')
            with self.lock:
                if not self._progress('CONNECTING', 'Connecting public prices and resynchronizing completed candles.'): return
                stream = getattr(self.service, 'stream', None)
                if not stream:
                    stream = self.stream_factory(self.service, sorted({s for s, _ in self.service.prepared}), sorted({i for _, i in self.service.prepared}))
                    self.service.stream = stream
                elif getattr(stream,'thread',None) and stream.thread.is_alive():
                    # A manual restart actively resynchronizes a surviving socket;
                    # don't wait for the next candle to reveal missing bars.
                    if hasattr(stream,'request_resync'):stream.request_resync()
                    else:stream.ready.clear();self.service.needs_resync=True
                stream.start()
            deadline = time.monotonic() + self.feed_timeout
            while not self.fresh():
                if self._wait(): return
                if time.monotonic() >= deadline:
                    raise ValueError('Fresh prices are unavailable. Feed reconnection continues; press Start after recovery.')
            with self.lock:
                if self.cancel.is_set() or self.service.stop.is_set(): return
                self.service.paper_mode = 'RUNNING'
                self._event('OBSERVING', 'Scanning Spot setups. GPT reviews only feasible candidates; the no-GPT comparator uses a separate virtual wallet.' if getattr(self.service,'spot',None) else 'Paper observation started. Waiting for an eligible setup; GPT assessment is disabled, so model-dependent entries remain blocked.')
        except Exception as exc:
            with self.lock:
                if not self.cancel.is_set():
                    self.service.pause()
                    self._event('BLOCKED', str(exc))

    def pause(self, emergency=False):
        with self.lock:
            self.cancel.set()
            self.service.pause()
            self._event('STOPPED' if emergency else 'PAUSED',
                'New entries stopped. Existing paper exits continue while fresh prices are available. No position was sold.' if emergency else
                'New entries paused. Price monitoring and existing paper exits continue.')

    def snapshot(self):
        with self.lock:
            busy = bool(self.thread and self.thread.is_alive())
            feed = self.fresh()
            signals=self.service.signal_health() if hasattr(self.service,'signal_health') else []
            signals_ok=bool(signals) and all(x['fresh'] for x in signals)
            stream=getattr(self.service,'stream',None)
            symbols={s for s,_ in self.service.prepared};now=time.time()
            quotes_ok=bool(stream and stream.ready.is_set() and symbols) and all(s in self.service.last_feed and 0<=now-self.service.last_feed[s]['received_at']<=3 for s in symbols)
            stage, message = self.stage, self.message
            if not busy and self.service.paper_mode == 'PAUSED' and stage == 'OBSERVING':
                stage, message = ('RECOVERED', 'Market connection recovered. Entries remain paused; press Start agent to resume.') if feed else ('RECOVERY', 'Observation paused after a data interruption. Reconnecting automatically; wait for fresh prices before pressing Start.')
            return {'stage': stage, 'message': message, 'busy': busy, 'stopping': busy and self.cancel.is_set(),
                    'connection':stream.status() if stream and hasattr(stream,'status') else None,
                    'market_connected': quotes_ok, 'analysis_current':signals_ok, 'live_blockers': LIVE_BLOCKERS,
                    'activity': list(reversed(self.events)),
                    'checks': [
                        {'name': 'Historical research', 'ok': bool(self.service.prepared), 'detail': 'Available' if self.service.prepared else 'Prepared automatically on Start'},
                        {'name': 'Market prices', 'ok': quotes_ok, 'detail': 'Fresh quotes' if quotes_ok else 'Waiting for fresh quotes'},
                        {'name': 'Candle analysis', 'ok': signals_ok, 'detail': 'Completed candles current' if signals_ok else 'Waiting for current completed candles'},
                        {'name': 'GPT assessment', 'ok': bool(getattr(self.service,'spot',None) and self.service.spot.analyst.economy.enabled), 'detail': 'Candidate-only requests; 2/hour, 12/day' if getattr(self.service,'spot',None) and self.service.spot.analyst.economy.enabled else 'Connect ChatGPT; GPT entries blocked'},
                        {'name': 'Real trading', 'ok': False, 'detail': 'Locked; no exchange orders can be sent'},
                    ]}

    def close(self):
        self.cancel.set()
        if self.thread: self.thread.join()
