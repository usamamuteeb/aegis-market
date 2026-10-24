"""Bounded GPT veto/approval on supplied Spot evidence, never a risk authority."""
import math
import time
from .model import Economy
from .store import identity,encode
from aegis.domain import Blocked

FIELDS=('assessment_id','candidate_id','symbol','as_of','valid_until','decision','reason','invalidation_conditions','evidence_ids')
SCHEMA={'type':'object','additionalProperties':False,'required':list(FIELDS),'properties':{
 **{k:{'type':'string'} for k in ('assessment_id','candidate_id','symbol','reason')},
 **{k:{'type':'number'} for k in ('as_of','valid_until')},'decision':{'type':'string','enum':['BUY','WAIT']},
 **{k:{'type':'array','items':{'type':'string'}} for k in ('invalidation_conditions','evidence_ids')}}}


def validate(reply,packet,now):
    if not isinstance(reply,dict) or set(reply)!=set(FIELDS):raise Blocked('Malformed GPT assessment; WAIT')
    c=packet['candidate']
    if reply['candidate_id']!=c['id'] or reply['symbol']!=c['symbol']:raise Blocked('GPT candidate identity mismatch; WAIT')
    for k in ('as_of','valid_until'):
        if isinstance(reply[k],bool) or not isinstance(reply[k],(float,int)) or not math.isfinite(reply[k]):raise Blocked('Invalid GPT timestamp; WAIT')
    if reply['as_of']!=packet['as_of'] or not now<reply['valid_until']<=c['expires_at']:raise Blocked('GPT assessment expired; WAIT')
    if reply['decision'] not in ('BUY','WAIT'):raise Blocked('Unsupported GPT decision; WAIT')
    if not isinstance(reply['assessment_id'],str) or not 1<=len(reply['assessment_id'])<=100:raise Blocked('Invalid assessment identifier')
    if not isinstance(reply['reason'],str) or not 1<=len(reply['reason'])<=600:raise Blocked('Invalid GPT explanation')
    for k in ('invalidation_conditions','evidence_ids'):
        if not isinstance(reply[k],list) or not 1<=len(reply[k])<=6 or any(not isinstance(x,str) or not 1<=len(x)<=180 for x in reply[k]):raise Blocked('Invalid GPT evidence')
    if not set(reply['evidence_ids'])<=set(packet['evidence_ids']):raise Blocked('Invented GPT evidence; WAIT')
    return reply


class SpotAnalyst:
    def __init__(self,store,clock):
        self.store=store;self.clock=clock;self.adapter=None;self.model=None
        self.economy=Economy(store,enabled=False,hourly=2,daily=12,cooldown=1800)

    def attach(self,adapter,model):
        if not isinstance(model,str) or not model.lower().startswith('gpt-6-sol'):raise Blocked('Choose the exact GPT-6 Sol identifier from your account catalog')
        self.adapter=adapter;self.model=model;self.economy.enabled=True

    def assess(self,packet):
        model,adapter=self.model,self.adapter
        key=identity(['spot-evidence-v1',packet['candidate']['id'],model]);rid=None;now=self.clock()
        try:
            if not self.adapter or not self.economy.enabled:raise Blocked('Connect ChatGPT and select GPT-6 Sol; model entries remain blocked')
            if now>=packet['candidate']['expires_at']:raise Blocked('Candidate expired before GPT dispatch')
            if len(encode(packet).encode())>8000:raise Blocked('Market snapshot exceeds the local context bound')
            cached=self.economy.cached(key,now)
            if cached:return validate(cached,packet,now)
            rid=self.economy.reserve(key,packet['candidate']['symbol'],time.time())
            reply,usage=adapter.assess_evidence(packet,model,SCHEMA,max_output_tokens=350,reasoning='low',timeout=8)
            reply=validate(reply,packet,self.clock());self.economy.finish(rid,usage,cache=reply)
            self.store.append('assessments',{**reply,'source':'spot-gpt-forward','model':model},symbol=reply['symbol'],as_of=self.clock())
            return reply
        except Exception as exc:
            if rid:self.economy.finish(rid,getattr(exc,'usage',None),error='SPOT_ASSESSMENT_UNAVAILABLE')
            reason=str(exc) if isinstance(exc,Blocked) else 'GPT unavailable or invalid response; WAIT'
            self.store.append('assessments',{'decision':'WAIT','reason':reason,'candidate_id':packet['candidate']['id'],'source':'spot-gpt-forward','model':model,'status':'FAILED_OR_BLOCKED'},symbol=packet['candidate']['symbol'],as_of=self.clock())
            return {'decision':'WAIT','reason':reason}
