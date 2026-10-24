"""Durable PostgreSQL implementation of the existing evidence-store interface."""
import json,re,threading,time,ssl
from pathlib import Path
from aegis.intelligence.store import ResearchStore,TABLES,encode

class Row(dict):
    def __getitem__(self,key):return list(self.values())[key] if isinstance(key,int) else super().__getitem__(key)

class Cursor:
    def __init__(self,cursor):self.cursor=cursor
    def fetchone(self):
        row=self.cursor.fetchone();return Row(row) if row is not None else None
    def fetchall(self):return [Row(r) for r in self.cursor.fetchall()]
    def __iter__(self):return iter(self.fetchall())

def sql(text):
    text=text.replace('BEGIN IMMEDIATE','BEGIN').replace('?','%s')
    ignore='INSERT OR IGNORE' in text;text=text.replace('INSERT OR IGNORE','INSERT')
    if ignore:text+=' ON CONFLICT DO NOTHING'
    if 'INSERT OR REPLACE INTO assessment_cache' in text:
        text=text.replace('INSERT OR REPLACE','INSERT')+' ON CONFLICT(key) DO UPDATE SET expires_at=excluded.expires_at,body=excluded.body'
    return re.sub(r'\b(interval|end)\b',lambda m:'"'+m[0]+'"',text)

class Database:
    def __init__(self,connection):self.connection=connection;self.transactions=[]
    def execute(self,text,args=()):return Cursor(self.connection.execute(sql(text),args))
    def __enter__(self):
        tx=self.connection.transaction();tx.__enter__();self.transactions.append(tx);return self
    def __exit__(self,*exc):return self.transactions.pop().__exit__(*exc)
    def commit(self):self.connection.commit()
    def rollback(self):self.connection.rollback()
    def close(self):self.connection.close()

class PostgresStore(ResearchStore):
    def __init__(self,url):
        import psycopg
        from psycopg.rows import dict_row
        from urllib.parse import urlsplit,parse_qs
        parsed=urlsplit(url)
        if parsed.scheme not in ('postgres','postgresql'):raise ValueError('Use a PostgreSQL connection URL')
        if '-pooler.' in (parsed.hostname or '') or parsed.port==6543 or parse_qs(parsed.query).get('pgbouncer')==['true']:raise ValueError('Use a direct/session connection for the worker, not a transaction-pooling URL')
        ca=ssl.get_default_verify_paths().cafile
        if not ca:
            import certifi
            ca=certifi.where()
        self.directory=Path('/tmp/aegis-runtime');self.lock=threading.RLock()
        self.db=Database(psycopg.connect(url,autocommit=True,row_factory=dict_row,connect_timeout=10,sslmode='verify-full',sslrootcert=ca))
        with self.db.connection.transaction():
            self.db.execute('SELECT pg_advisory_xact_lock(194512711)')
            for table in TABLES:
                self.db.execute(f'CREATE TABLE IF NOT EXISTS {table}(id TEXT PRIMARY KEY,symbol TEXT NOT NULL,as_of DOUBLE PRECISION NOT NULL,created_at DOUBLE PRECISION NOT NULL,available_at DOUBLE PRECISION NOT NULL,version TEXT NOT NULL,body TEXT NOT NULL)')
                self.db.execute(f'CREATE INDEX IF NOT EXISTS {table}_causal ON {table}(symbol,available_at,as_of)')
            definitions=[
              'candle_revisions(symbol TEXT,interval TEXT,open_ms BIGINT,revision INTEGER,close_ms BIGINT,fetch_at DOUBLE PRECISION,body TEXT,PRIMARY KEY(symbol,interval,open_ms,revision))',
              'candle_heads(symbol TEXT,interval TEXT,open_ms BIGINT,revision INTEGER,PRIMARY KEY(symbol,interval,open_ms))',
              'dataset_members(dataset_id TEXT,open_ms BIGINT,revision INTEGER,PRIMARY KEY(dataset_id,open_ms))',
              'cursors(key TEXT PRIMARY KEY,body TEXT NOT NULL)',
              'inspections(id BIGSERIAL PRIMARY KEY,dataset_key TEXT,start DOUBLE PRECISION,end DOUBLE PRECISION,viewed_at DOUBLE PRECISION,run_id TEXT)',
              'holdouts(id TEXT PRIMARY KEY,start DOUBLE PRECISION,end DOUBLE PRECISION,registered_at DOUBLE PRECISION,body TEXT)',
              'model_calls(id TEXT PRIMARY KEY,cache_key TEXT UNIQUE,symbol TEXT,started_at DOUBLE PRECISION,finished_at DOUBLE PRECISION,status TEXT,usage TEXT,error_code TEXT)',
              'assessment_cache(key TEXT PRIMARY KEY,expires_at DOUBLE PRECISION,body TEXT)',
              'state(key TEXT PRIMARY KEY,body TEXT NOT NULL)',
              'cloud_commands(id TEXT PRIMARY KEY,created_at DOUBLE PRECISION NOT NULL,status TEXT NOT NULL,body TEXT NOT NULL,result TEXT)',
            ]
            for definition in definitions:self.db.execute('CREATE TABLE IF NOT EXISTS '+definition)
    def own_worker(self):
        with self.lock:return bool(self.db.execute('SELECT pg_try_advisory_lock(194512712) AS owned').fetchone()['owned'])

