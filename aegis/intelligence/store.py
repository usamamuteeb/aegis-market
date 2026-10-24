"""Versioned research storage. Append evidence; never touch the financial ledger."""
import hashlib
import json
import sqlite3
import threading
import time
from pathlib import Path

TABLES = ('datasets','coin_profiles','feature_snapshots','regime_snapshots','predictions',
          'prediction_outcomes','assessments','decisions','trade_episodes','hypotheses',
          'strategy_versions','evaluation_runs','memory_items','observations','latencies')

def encode(value): return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)
def identity(value): return hashlib.sha256(encode(value).encode()).hexdigest()[:32]
def source_fingerprint():
    root=Path(__file__).resolve().parent
    files=sorted(root.glob('*.py'))+[root.parent/n for n in ('strategy.py','risk.py','config.py','chatgpt.py')]
    return hashlib.sha256(b''.join(p.name.encode()+b'\0'+p.read_bytes() for p in files)).hexdigest()

def safe_directory(directory):
    path=Path(directory).resolve()
    production=Path('C:/Users/HP/Music/Aegis-Crypto-Agent/binance-v3').resolve()
    if path==production or path.is_relative_to(production) or 'data' in path.parts or path.name.lower() in ('binance-v3','binance-v2'):
        raise ValueError('Choose a separate research workspace, outside production and any data directory')
    path.mkdir(parents=True,exist_ok=True)
    marker=path/'research-workspace.json'
    if not marker.exists():
        if any(path.iterdir()): raise ValueError('New research workspace must be empty')
        marker.write_text(encode({'kind':'aegis-isolated-research','schema':1}),encoding='utf-8')
    if json.loads(marker.read_text(encoding='utf-8')).get('kind')!='aegis-isolated-research': raise ValueError('Unknown workspace')
    return path

class ResearchStore:
    def __init__(self,directory):
        self.directory=safe_directory(directory);self.lock=threading.RLock()
        self.db=sqlite3.connect(self.directory/'research.sqlite3',timeout=30,check_same_thread=False)
        self.db.row_factory=sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL');self.db.execute('PRAGMA synchronous=FULL')
        with self.db:
            self.db.execute('CREATE TABLE IF NOT EXISTS migrations(version INTEGER PRIMARY KEY, applied_at REAL NOT NULL)')
            if not self.db.execute('SELECT 1 FROM migrations WHERE version=1').fetchone():
                for table in TABLES:
                    self.db.execute(f'''CREATE TABLE {table}(id TEXT PRIMARY KEY,symbol TEXT NOT NULL,as_of REAL NOT NULL,
                        created_at REAL NOT NULL,available_at REAL NOT NULL,version TEXT NOT NULL,body TEXT NOT NULL)''')
                    self.db.execute(f'CREATE INDEX {table}_causal ON {table}(symbol,available_at,as_of)')
                self.db.executescript('''
                    CREATE TABLE candle_revisions(symbol TEXT,interval TEXT,open_ms INTEGER,revision INTEGER,
                      close_ms INTEGER,fetch_at REAL,body TEXT,PRIMARY KEY(symbol,interval,open_ms,revision));
                    CREATE TABLE candle_heads(symbol TEXT,interval TEXT,open_ms INTEGER,revision INTEGER,
                      PRIMARY KEY(symbol,interval,open_ms));
                    CREATE TABLE dataset_members(dataset_id TEXT,open_ms INTEGER,revision INTEGER,PRIMARY KEY(dataset_id,open_ms));
                    CREATE TABLE cursors(key TEXT PRIMARY KEY,body TEXT NOT NULL);
                    CREATE TABLE inspections(id INTEGER PRIMARY KEY,dataset_key TEXT,start REAL,end REAL,viewed_at REAL,run_id TEXT);
                    CREATE TABLE holdouts(id TEXT PRIMARY KEY,start REAL,end REAL,registered_at REAL,body TEXT);
                    CREATE TABLE model_calls(id TEXT PRIMARY KEY,cache_key TEXT UNIQUE,symbol TEXT,started_at REAL,
                      finished_at REAL,status TEXT,usage TEXT,error_code TEXT);
                    CREATE TABLE assessment_cache(key TEXT PRIMARY KEY,expires_at REAL,body TEXT);
                    CREATE TABLE state(key TEXT PRIMARY KEY,body TEXT NOT NULL);
                ''')
                self.db.execute('INSERT INTO migrations VALUES(1,?)',(time.time(),))
        if self.db.execute('SELECT MAX(version) FROM migrations').fetchone()[0]!=1: raise ValueError('Unsupported research schema')

    def append(self,table,body,*,symbol='',as_of=None,created_at=None,available_at=None,version='v1',record_id=None):
        if table not in TABLES: raise ValueError('Unknown evidence table')
        now=time.time() if created_at is None else created_at
        as_of=now if as_of is None else as_of; available_at=max(now,as_of) if available_at is None else available_at
        rid=record_id or identity({'table':table,'body':body,'as_of':as_of,'created_at':now,'version':version})
        with self.lock,self.db:
            old=self.db.execute(f'SELECT body FROM {table} WHERE id=?',(rid,)).fetchone()
            if old and old[0]!=encode(body): raise ValueError('Immutable record correction requires a new ID and supersedes reference')
            self.db.execute(f'INSERT OR IGNORE INTO {table} VALUES(?,?,?,?,?,?,?)',(rid,symbol,as_of,now,available_at,version,encode(body)))
        return rid

    def records(self,table,*,symbol=None,at=None,limit=100):
        if table not in TABLES: raise ValueError('Unknown evidence table')
        where=[];args=[]
        if symbol is not None:where.append('symbol=?');args.append(symbol)
        if at is not None:where.extend(['available_at<=?','created_at<=?','as_of<=?']);args.extend([at,at,at])
        args.append(limit)
        with self.lock:
            rows=self.db.execute(f'SELECT * FROM {table}'+(' WHERE '+' AND '.join(where) if where else '')+' ORDER BY as_of DESC,id LIMIT ?',args).fetchall()
        return [{**dict(row),'body':json.loads(row['body'])} for row in rows]

    def get(self,table,rid):
        if table not in TABLES:raise ValueError('Unknown evidence table')
        with self.lock:row=self.db.execute(f'SELECT * FROM {table} WHERE id=?',(rid,)).fetchone()
        return {**dict(row),'body':json.loads(row['body'])} if row else None

    def state(self,key,default=None):
        with self.lock:row=self.db.execute('SELECT body FROM state WHERE key=?',(key,)).fetchone()
        return json.loads(row[0]) if row else default

    def put_state(self,key,body):
        with self.lock,self.db:self.db.execute('INSERT INTO state VALUES(?,?) ON CONFLICT(key) DO UPDATE SET body=excluded.body',(key,encode(body)))

    def close(self):self.db.close()
