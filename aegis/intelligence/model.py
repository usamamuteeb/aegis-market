"""Explicitly disabled by default; durable economy gates before any model access."""
import json
import math
import time
from .store import encode,identity
from aegis.domain import Blocked

PROMPT_VERSION='evidence-v1'
FIELDS=('assessment_id','candidate_id','symbol','as_of','valid_until','action','prediction_id',
        'evidence_ids','reason_codes','rationale','invalidation_conditions','limitations')
SCHEMA={'type':'object','additionalProperties':False,'required':list(FIELDS),'properties':{
    **{k:{'type':'string'} for k in ('assessment_id','candidate_id','symbol','prediction_id','rationale')},
    'as_of':{'type':'number'},'valid_until':{'type':'number'},'action':{'type':'string','enum':['BUY','WAIT','EXIT']},
    **{k:{'type':'array','items':{'type':'string'}} for k in ('evidence_ids','reason_codes','invalidation_conditions','limitations')}}}

def validate(reply,packet,now):
    if not isinstance(reply,dict) or set(reply)!=set(FIELDS):raise Blocked('Malformed evidence assessment; WAIT')
    c=packet['candidate']
    if reply['candidate_id']!=c['id'] or reply['symbol']!=c['symbol'] or reply['prediction_id']!=packet['prediction']['id']:raise Blocked('Assessment evidence identity mismatch; WAIT')
    if reply['action'] not in ('BUY','WAIT','EXIT') or reply['action']=='EXIT' and not packet.get('position'):raise Blocked('Unsupported evidence action; WAIT')
    for k in ('as_of','valid_until'):
        if isinstance(reply[k],bool) or not isinstance(reply[k],(float,int)) or not math.isfinite(reply[k]):raise Blocked('Invalid evidence time; WAIT')
    if reply['as_of']!=packet['as_of'] or not now<reply['valid_until']<=c['expires_at']:raise Blocked('Expired or mismatched evidence time; WAIT')
    for k in ('assessment_id','rationale'):
        if not isinstance(reply[k],str) or not 1<=len(reply[k])<=(700 if k=='rationale' else 120):raise Blocked('Invalid evidence explanation; WAIT')
    for k in ('evidence_ids','reason_codes','invalidation_conditions','limitations'):
        if not isinstance(reply[k],list) or not 1<=len(reply[k])<=8 or any(not isinstance(x,str) or not 1<=len(x)<=220 for x in reply[k]):raise Blocked('Invalid evidence references; WAIT')
    if not set(reply['evidence_ids'])<=set(packet['evidence_ids']):raise Blocked('Invented evidence reference; WAIT')
    if reply['action']=='BUY' and not packet['prediction']['eligible']:raise Blocked('Forecast abstention cannot be overridden; WAIT')
    return reply

class Economy:
    def __init__(self,store,*,enabled=False,hourly=2,daily=12,cooldown=1800,max_observed_tokens=20000):
        self.store=store;self.enabled=enabled;self.hourly=hourly;self.daily=daily;self.cooldown=cooldown;self.max_observed_tokens=max_observed_tokens

    def reserve(self,key,symbol,now):
        if not self.enabled:raise Blocked('Runtime GPT inference is disabled; WAIT')
        db=self.store.db
        with self.store.lock:
            db.execute('BEGIN IMMEDIATE')
            try:
                rows=db.execute('SELECT * FROM model_calls WHERE started_at>?',(now-86400,)).fetchall()
                if any(r['started_at']>now for r in rows):raise Blocked('Clock moved backwards; model dispatch blocked')
                if db.execute('SELECT 1 FROM model_calls WHERE cache_key=?',(key,)).fetchone():raise Blocked('Unchanged candidate already attempted; no automatic model retry')
                if any(r['status']=='PENDING' and now-r['started_at']<120 for r in rows):raise Blocked('One model request is already reserved')
                if len(rows)>=self.daily or sum(r['started_at']>now-3600 for r in rows)>=self.hourly:raise Blocked('Rolling model call budget exhausted; WAIT')
                if any(r['symbol']==symbol and r['started_at']>now-self.cooldown for r in rows):raise Blocked('Per-symbol model cooldown; WAIT')
                observed=sum(json.loads(r['usage'] or '{}').get('total_tokens') or 0 for r in rows)
                if observed>=self.max_observed_tokens:raise Blocked('Observed local token ceiling exhausted; WAIT')
                rid=identity([key,now]);db.execute('INSERT INTO model_calls VALUES(?,?,?,?,?,?,?,?)',(rid,key,symbol,now,None,'PENDING',None,None));db.commit();return rid
            except Exception:db.rollback();raise

    def finish(self,rid,usage=None,error=None,cache=None):
        now=time.time();usage=usage or {}
        # Preserve reported nested reasoning/cached/tool usage; unknown stays unknown.
        body={'input_tokens':usage.get('input_tokens'),'output_tokens':usage.get('output_tokens'),
              'total_tokens':usage.get('total_tokens'),'input_tokens_details':usage.get('input_tokens_details'),
              'output_tokens_details':usage.get('output_tokens_details'),'reported_usage':usage,
              'codex_credits':None,'account_remaining_allowance':None}
        with self.store.lock,self.store.db:
            row=self.store.db.execute('SELECT cache_key FROM model_calls WHERE id=?',(rid,)).fetchone()
            self.store.db.execute('UPDATE model_calls SET finished_at=?,status=?,usage=?,error_code=? WHERE id=? AND status=?',(now,'FAILED' if error else 'COMPLETED',encode(body),error,rid,'PENDING'))
            if cache and not error:self.store.db.execute('INSERT OR REPLACE INTO assessment_cache VALUES(?,?,?)',(row[0],cache['valid_until'],encode(cache)))

    def cached(self,key,now):
        with self.store.lock:row=self.store.db.execute('SELECT body FROM assessment_cache WHERE key=? AND expires_at>?',(key,now)).fetchone()
        if row:
            count=self.store.state('cache_hits',0);self.store.put_state('cache_hits',count+1)
        return json.loads(row[0]) if row else None

    def status(self,now=None):
        now=time.time() if now is None else now
        with self.store.lock:rows=self.store.db.execute('SELECT * FROM model_calls WHERE started_at>?',(now-86400,)).fetchall()
        return {'enabled':self.enabled,'hourly_ceiling':self.hourly,'daily_ceiling':self.daily,'cooldown_seconds':self.cooldown,
                'calls_rolling_24h':len(rows),'calls_rolling_hour':sum(r['started_at']>now-3600 for r in rows),
                'failed_attempts':sum(r['status']=='FAILED' for r in rows),'cache_hits':self.store.state('cache_hits',0),
                'observed_usage':[json.loads(r['usage']) for r in rows if r['usage']],
                'codex_credits':None,'credit_label':'Unknown: official account-wide credit telemetry is not supplied by this integration',
                'speed':'Standard (no accelerated tier requested)','reasoning':'low when supported; rejection means WAIT',
                'scope':'Persistent application call ceilings; external sessions and unreported reasoning prevent an account-wide credit guarantee'}

class EvidenceAnalyst:
    def __init__(self,store,adapter=None,selected_model=None,*,enabled=False,clock=time.time):
        self.store=store;self.adapter=adapter;self.model=selected_model;self.economy=Economy(store,enabled=enabled);self.clock=clock

    def assess(self,packet):
        now=self.clock();c=packet['candidate'];key=identity([packet,self.model,PROMPT_VERSION])
        if not self.economy.enabled:
            wait={'action':'WAIT','reason':'Runtime GPT inference disabled; WAIT','candidate_id':c['id'],'as_of':packet['as_of']}
            self.store.append('assessments',wait,symbol=c['symbol'],as_of=packet['as_of']);return wait,'WAIT'
        cached=self.economy.cached(key,now)
        if cached:return validate(cached,packet,now),'CACHE'
        rid=None
        try:
            if not self.economy.enabled:raise Blocked('Runtime GPT inference disabled; WAIT')
            # Preserve the exact caller-selected account catalog slug, never another GPT variant.
            if not self.model or not self.model.lower().startswith('gpt-6-sol') or not self.adapter:raise Blocked('Account-supported GPT-6 Sol selection unavailable; WAIT')
            if len(encode(packet).encode('utf-8'))>9000:raise Blocked('Evidence packet exceeds conservative ~3000-token context ceiling; WAIT')
            if now>=c['expires_at'] or not packet['prediction']['eligible']:raise Blocked('Candidate expired or forecast abstained; WAIT')
            rid=self.economy.reserve(key,c['symbol'],now)
            reply,usage=self.adapter.assess_evidence(packet,self.model,SCHEMA,max_output_tokens=500,reasoning='low',timeout=20)
            reply=validate(reply,packet,self.clock())
            self.store.append('assessments',reply,symbol=c['symbol'],as_of=packet['as_of'],record_id=reply['assessment_id'])
            self.economy.finish(rid,usage,cache=reply)
            return reply,'MODEL'
        except Exception as exc:
            if rid:self.economy.finish(rid,getattr(exc,'usage',None),error='MODEL_UNAVAILABLE_OR_INVALID')
            reason=str(exc) if isinstance(exc,Blocked) else 'Evidence assessment unavailable; WAIT'
            wait={'action':'WAIT','reason':reason,'candidate_id':c['id'],'prediction_id':packet['prediction']['id'],
                  'evidence_ids':packet['evidence_ids'],'as_of':packet['as_of'],'valid_until':c['expires_at']}
            self.store.append('assessments',wait,symbol=c['symbol'],as_of=packet['as_of'])
            return wait,'WAIT'
