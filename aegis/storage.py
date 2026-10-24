import json
import sqlite3
import threading
import time
from pathlib import Path
from aegis.config import DEFAULTS
from aegis.domain import Blocked

class Store:
    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.lock = threading.RLock()
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
          CREATE TABLE IF NOT EXISTS settings(id INTEGER PRIMARY KEY CHECK(id=1), body TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS state(id INTEGER PRIMARY KEY CHECK(id=1), body TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS intents(id TEXT PRIMARY KEY, kind TEXT NOT NULL, status TEXT NOT NULL, at REAL NOT NULL, body TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY, at REAL NOT NULL, kind TEXT NOT NULL, body TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS exchange_trades(symbol TEXT NOT NULL, id INTEGER NOT NULL, body TEXT NOT NULL, PRIMARY KEY(symbol,id));
          CREATE TABLE IF NOT EXISTS sessions(id INTEGER PRIMARY KEY, at REAL NOT NULL, body TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS wallet_marks(at INTEGER PRIMARY KEY, value TEXT NOT NULL, complete INTEGER NOT NULL);
        ''')
        self.settings = self._get('settings') or dict(DEFAULTS)
        self.state = self._get('state') or {'position': None, 'mode': 'PAUSED', 'generation': 0, 'seen': {},
                   'daily_date': '', 'daily_realized': '0', 'realized': '0', 'daily_entries': 0,
                   'consecutive_losses': 0, 'daily_halted': False, 'last_exit_at': 0,
                   'peak_bot_equity': None, 'usage': {'calls': 0, 'input_tokens': 0, 'output_tokens': 0}}
        if self.state['mode'] != 'EMERGENCY':
            self.state['mode'] = 'PAUSED'
        self.state['generation'] += 1
        self.save(self.state, event=('system', {'message': 'Worker started; new entries are paused. Existing exchange protection is reconciled.'}))

    def _get(self, table):
        row = self.db.execute(f'SELECT body FROM {table} WHERE id=1').fetchone()
        return json.loads(row[0]) if row else None

    def save(self, state, event=None, intent=None, settings=None):
        encode = lambda v: json.dumps(v, allow_nan=False)
        with self.lock, self.db:
            self.db.execute('INSERT OR REPLACE INTO state VALUES(1,?)', (encode(state),))
            if settings is not None:
                self.db.execute('INSERT OR REPLACE INTO settings VALUES(1,?)', (encode(settings),))
            if event:
                self.db.execute('INSERT INTO events(at,kind,body) VALUES(?,?,?)', (time.time(), event[0], encode(event[1])))
            if intent:
                existing = self.db.execute('SELECT body FROM intents WHERE id=?', (intent['id'],)).fetchone()
                if existing and json.loads(existing[0])['kind'] != intent['kind']:
                    raise Blocked('Order intent identity conflict')
                self.db.execute('INSERT INTO intents VALUES(?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET status=excluded.status,body=excluded.body',
                                (intent['id'], intent['kind'], intent['status'], intent['created_at'], encode(intent)))
        self.state = state
        if settings is not None:
            self.settings = settings

    def event(self, kind, message, **fields):
        with self.lock:
            self.save(self.state, (kind, {'message': message, **fields}))

    def intent(self, key):
        with self.lock:
            row = self.db.execute('SELECT body FROM intents WHERE id=?', (key,)).fetchone()
            return json.loads(row[0]) if row else None

    def intents(self, statuses=None):
        with self.lock:
            rows = [json.loads(r[0]) for r in self.db.execute('SELECT body FROM intents ORDER BY at DESC')]
            return [r for r in rows if r['status'] in statuses] if statuses else rows

    def events(self, limit=100):
        with self.lock:
            return [{'id': r[0], 'at': r[1], 'kind': r[2], **json.loads(r[3])}
                    for r in self.db.execute('SELECT id,at,kind,body FROM events ORDER BY id DESC LIMIT ?', (limit,))]

    def merge_trades(self, symbol, rows):
        with self.lock, self.db:
            for row in rows:
                self.db.execute('INSERT OR REPLACE INTO exchange_trades VALUES(?,?,?)', (symbol, row['id'], json.dumps({**row, 'symbol': symbol})))

    def trades(self, symbol=None, limit=100):
        with self.lock:
            if symbol:
                rows = self.db.execute('SELECT body FROM exchange_trades WHERE symbol=? ORDER BY id DESC LIMIT ?', (symbol, limit))
            else:
                rows = self.db.execute('SELECT body FROM exchange_trades ORDER BY json_extract(body, "$.time") DESC LIMIT ?', (limit,))
            return [json.loads(r[0]) for r in rows]

    def wallet_mark(self, value, complete):
        with self.lock, self.db:
            # One observed point per 30-second bucket; no interpolation or invented history.
            self.db.execute('INSERT OR REPLACE INTO wallet_marks VALUES(?,?,?)', (int(time.time()//30)*30, str(value), int(complete)))

    def wallet_curve(self):
        with self.lock:
            rows = list(self.db.execute('SELECT at,value,complete FROM wallet_marks ORDER BY at DESC LIMIT 2016'))
        return [{'at':r[0], 'value':r[1], 'complete':bool(r[2])} for r in reversed(rows)]
