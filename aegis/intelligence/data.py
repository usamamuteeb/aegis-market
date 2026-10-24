"""Incremental, public-only Binance OHLCV, immutable revisions and explicit coverage."""
import json
import math
import time
from decimal import Decimal
from urllib.request import Request,urlopen
from urllib.error import HTTPError
from .store import encode,identity

INTERVALS={'1m':60,'5m':300,'15m':900,'1h':3600,'4h':14400,'1d':86400}
PUBLIC={'exchangeInfo','klines','depth','trades','time'}

class PublicDataError(Exception):pass

class PublicClient:
    def __init__(self,timeout=12,base='https://data-api.binance.vision'):
        if base not in ('https://data-api.binance.vision','https://api.binance.com'):raise ValueError('Documented public Binance host required')
        self.base=base;self.timeout=timeout;self.not_before=0
    def get(self,route,**params):
        from urllib.parse import urlencode
        if route not in PUBLIC:raise PublicDataError('Only documented public research routes are permitted')
        if time.time()<self.not_before:raise PublicDataError('Public rate-limit backoff active; ingestion is resumable')
        request=Request(self.base+'/api/v3/'+route+'?'+urlencode(params),headers={'User-Agent':'Aegis-Public-Research/1'})
        try:
            with urlopen(request,timeout=self.timeout) as response:return json.load(response)
        except HTTPError as exc:
            if exc.code in (418,429):
                try:delay=float(exc.headers.get('Retry-After',60))
                except ValueError:delay=60
                self.not_before=time.time()+max(1,delay)
            raise PublicDataError('Public data HTTP '+str(exc.code)) from None
        except Exception:raise PublicDataError('Public data unavailable; retry the resumable ingestion later') from None

def parse_bar(row,interval,now):
    step=INTERVALS[interval]*1000
    if not isinstance(row,list) or len(row)<7:raise PublicDataError('Malformed OHLCV row')
    try:
        start,end=int(row[0]),int(row[6]);o,h,l,c,v=[Decimal(str(x)) for x in row[1:6]]
        if any(not x.is_finite() for x in (o,h,l,c,v)):raise ValueError()
        if start%step or end!=start+step-1 or not 0<l<=min(o,c)<=max(o,c)<=h or v<0:raise ValueError()
    except Exception:raise PublicDataError('Invalid UTC candle or OHLCV values') from None
    if end>=int(now*1000):return None
    return {'open_ms':start,'close_ms':end,'open':str(o),'high':str(h),'low':str(l),'close':str(c),'volume':str(v)}

def numeric(rows):
    return [{'open_time':r['open_ms']/1000,'close_time':r['close_ms']/1000,
             **{k:float(r[k]) for k in ('open','high','low','close','volume')}} for r in rows]

class Ingestor:
    def __init__(self,store,client=None):self.store=store;self.client=client or PublicClient()
    def metadata(self,symbol,now):
        response=self.client.get('exchangeInfo',symbol=symbol)
        records=response.get('symbols',[])
        if len(records)!=1:raise PublicDataError('Symbol identity unavailable')
        m=records[0]
        previous=next((r for r in self.store.records('observations',symbol=symbol,limit=100) if r['version']=='symbol-identity-v1'),None)
        observed={k:m.get(k) for k in ('symbol','baseAsset','quoteAsset','status','isSpotTradingAllowed','ocoAllowed','orderTypes','filters')}
        observed.update(source='Binance Global public exchangeInfo',observed_at=now,listing_date=None,
                        previous_observation=previous['id'] if previous else None,
                        changed_fields=[k for k in ('symbol','baseAsset','quoteAsset','status','filters','orderTypes') if previous and previous['body'].get(k)!=m.get(k)],
                        limitation='Observed status/identity changes only; original listing date is not supplied')
        self.store.append('observations',observed,symbol=symbol,as_of=now,version='symbol-identity-v1')
        if m.get('symbol')!=symbol or m.get('quoteAsset')!='USDT' or m.get('baseAsset')+'USDT'!=symbol or m.get('status')!='TRADING' or m.get('isSpotTradingAllowed') is not True:
            raise PublicDataError('Only verified TRADING Binance Global USDT Spot pairs are supported')
        body={k:m[k] for k in ('symbol','baseAsset','quoteAsset','status','isSpotTradingAllowed','filters')}
        body.update(ocoAllowed=m.get('ocoAllowed',False),orderTypes=m.get('orderTypes',[]))
        body.update(source='Binance Global public exchangeInfo',observed_at=now,listing_date=None,
                    limitation='First returned candle is not proof of original listing date or complete exchange history')
        return body

    def ingest(self,symbol,interval,start,end,*,max_pages=100,now=None,cancel=None,overlap_bars=2):
        if interval not in INTERVALS or not 0<=start<end:raise ValueError('Invalid research window')
        now=time.time() if now is None else now;end=min(end,now)
        metadata=self.metadata(symbol,now);step=INTERVALS[interval]*1000
        start_ms=int(start*1000)//step*step;end_ms=int(end*1000)//step*step
        key=identity([symbol,interval,start_ms,end_ms])
        with self.store.lock:
            prior=self.store.db.execute('SELECT body FROM cursors WHERE key=?',(key,)).fetchone()
            highest=self.store.db.execute('SELECT MAX(open_ms) FROM candle_heads WHERE symbol=? AND interval=? AND open_ms>=? AND open_ms<?',(symbol,interval,start_ms,end_ms)).fetchone()[0]
            present_heads={r[0] for r in self.store.db.execute('SELECT open_ms FROM candle_heads WHERE symbol=? AND interval=? AND open_ms>=? AND open_ms<?',(symbol,interval,start_ms,end_ms))}
        first_missing=next((t for t in range(start_ms,end_ms,step) if t not in present_heads),end_ms)
        cursor=min(json.loads(prior[0])['next_ms'],first_missing) if prior else first_missing
        if highest is not None and first_missing>highest:cursor=max(start_ms,highest-max(0,overlap_bars-1)*step)
        # A completed refresh rechecks the last two bars, recording exchange corrections.
        if cursor>=end_ms:cursor=max(start_ms,(highest or start_ms)-max(0,overlap_bars-1)*step)
        duplicates=corrections=unfinished=0;error=None;pages=0
        try:
            while cursor<end_ms and pages<max_pages and not(cancel and cancel.is_set()):
                raw=self.client.get('klines',symbol=symbol,interval=interval,startTime=cursor,endTime=end_ms-1,limit=1000)
                pages+=1
                if not isinstance(raw,list):raise PublicDataError('Expected public candle list')
                if not raw:break
                bars=[];page_max=cursor-step
                for row in raw:
                    bar=parse_bar(row,interval,now)
                    if bar is None:unfinished+=1;continue
                    if not start_ms<=bar['open_ms']<end_ms:continue
                    page_max=max(page_max,bar['open_ms']);bars.append(bar)
                if page_max<cursor:raise PublicDataError('Non-advancing pagination or only unfinished bars')
                with self.store.lock,self.store.db:
                    for bar in bars:
                        head=self.store.db.execute('SELECT r.revision,r.body FROM candle_heads h JOIN candle_revisions r USING(symbol,interval,open_ms,revision) WHERE h.symbol=? AND h.interval=? AND h.open_ms=?',(symbol,interval,bar['open_ms'])).fetchone()
                        if head and head['body']==encode(bar):duplicates+=1;continue
                        revision=head['revision']+1 if head else 1;corrections+=int(bool(head))
                        self.store.db.execute('INSERT INTO candle_revisions VALUES(?,?,?,?,?,?,?)',(symbol,interval,bar['open_ms'],revision,bar['close_ms'],now,encode(bar)))
                        self.store.db.execute('INSERT INTO candle_heads VALUES(?,?,?,?) ON CONFLICT(symbol,interval,open_ms) DO UPDATE SET revision=excluded.revision',(symbol,interval,bar['open_ms'],revision))
                    cursor=page_max+step
                    self.store.db.execute('INSERT INTO cursors VALUES(?,?) ON CONFLICT(key) DO UPDATE SET body=excluded.body',(key,encode({'next_ms':cursor,'updated_at':now})))
        except (PublicDataError,ValueError) as exc:error=str(exc)
        with self.store.lock:
            rows=self.store.db.execute('SELECT r.* FROM candle_heads h JOIN candle_revisions r USING(symbol,interval,open_ms,revision) WHERE h.symbol=? AND h.interval=? AND h.open_ms>=? AND h.open_ms<? ORDER BY h.open_ms',(symbol,interval,start_ms,end_ms)).fetchall()
        present={r['open_ms'] for r in rows};missing=[t for t in range(start_ms,end_ms,step) if t not in present]
        # Store gap ranges rather than silently filling missing prices.
        gaps=[]
        for t in missing:
            if gaps and gaps[-1][1]==t:gaps[-1][1]+=step
            else:gaps.append([t,t+step])
        digest=identity([[r['open_ms'],r['revision'],json.loads(r['body'])] for r in rows])
        body={'symbol':symbol,'base_asset':metadata['baseAsset'],'quote_asset':'USDT','venue':'Binance Global Spot',
              'interval':interval,'requested_start':start_ms/1000,'requested_end':end_ms/1000,'bars':len(rows),
              'first_open':rows[0]['open_ms']/1000 if rows else None,'last_close':rows[-1]['close_ms']/1000 if rows else None,
              'coverage_complete':not missing and bool(rows),'gaps_ms':gaps,'missing_bars':len(missing),
              'duplicates_observed':duplicates,'corrections_observed':corrections,'unfinished_excluded':unfinished,
              'cursor_ms':cursor,'pages':pages,'error':error,'fetched_at':now,'source':'GET /api/v3/klines',
              'content_hash':digest,'metadata':metadata,'version':'ohlcv-revisions-v1',
              'limitations':['Coverage refers only to the requested window.','No historical depth has been reconstructed.']}
        did=self.store.append('datasets',body,symbol=symbol,as_of=end_ms/1000,created_at=now)
        with self.store.lock,self.store.db:
            for r in rows:self.store.db.execute('INSERT OR IGNORE INTO dataset_members VALUES(?,?,?)',(did,r['open_ms'],r['revision']))
        return did,body

    def load(self,dataset_id):
        dataset=self.store.get('datasets',dataset_id)
        if not dataset:raise ValueError('Unknown dataset')
        d=dataset['body']
        with self.store.lock:
            rows=self.store.db.execute('SELECT r.body FROM dataset_members m JOIN candle_revisions r ON r.open_ms=m.open_ms AND r.revision=m.revision WHERE m.dataset_id=? AND r.symbol=? AND r.interval=? ORDER BY r.open_ms',(dataset_id,d['symbol'],d['interval'])).fetchall()
        return [json.loads(r[0]) for r in rows]

    def observe(self,symbol,now=None):
        now=time.time() if now is None else now
        depth=self.client.get('depth',symbol=symbol,limit=20);trades=self.client.get('trades',symbol=symbol,limit=20)
        if not depth.get('bids') or not depth.get('asks'):raise PublicDataError('Empty public order book')
        bid,ask=Decimal(depth['bids'][0][0]),Decimal(depth['asks'][0][0])
        if not 0<bid<=ask:raise PublicDataError('Invalid public book')
        body={'bid':str(bid),'ask':str(ask),'spread_bps':float((ask-bid)/((ask+bid)/2)*10000),
              'depth':depth,'recent_trades':trades,'received_at':now,'source_event_at':None,
              'limitation':'REST book has receive time, not a reconstructed historical source-event time'}
        return self.store.append('observations',body,symbol=symbol,as_of=now,version='prospective-book-v1'),body

def aggregate(rows,source,target,as_of):
    if INTERVALS[target]%INTERVALS[source]:raise ValueError('Timeframes must divide exactly')
    factor=INTERVALS[target]//INTERVALS[source];step=INTERVALS[target]*1000;groups={}
    for r in rows:
        if r['close_ms']/1000>=as_of:continue
        groups.setdefault(r['open_ms']//step*step,[]).append(r)
    result=[]
    for start,group in sorted(groups.items()):
        group=sorted(group,key=lambda r:r['open_ms'])
        if len(group)!=factor or [r['open_ms'] for r in group]!=[start+i*INTERVALS[source]*1000 for i in range(factor)]:continue
        if (start+step-1)/1000>=as_of:continue
        result.append({'open_ms':start,'close_ms':start+step-1,'open':group[0]['open'],'close':group[-1]['close'],
                       'high':str(max(Decimal(r['high']) for r in group)),'low':str(min(Decimal(r['low']) for r in group)),
                       'volume':str(sum(Decimal(r['volume']) for r in group))})
    return result
