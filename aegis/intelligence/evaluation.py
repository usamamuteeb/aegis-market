"""Fixed walk-forward experiment, purged labels and auditable holdout inspection."""
import math
import time
from statistics import mean,pstdev
from .features import features,candidate,POLICY
from .forecast import resolve,Predictor,chronological,scorecard,VERSION
from .data import INTERVALS
from .paper import PaperBroker,reference_book
from .store import identity,encode,source_fingerprint
from .memory import remember
from aegis.market import parse_rules

def examples(symbol,rows,interval,tick,*,fee=.001,spread_bps=10,slippage_bps=10):
    result=[]
    for i in range(65,len(rows)):
        prefix=rows[max(0,i-150):i];as_of=prefix[-1]['close_ms']/1000+.001
        try:
            f,r=features(prefix,interval,as_of)
            c=candidate(symbol,prefix,interval,as_of,float(prefix[-1]['close'])*(1+spread_bps/20000),tick)
        except ValueError:continue
        if not c['eligible']:continue
        label=resolve(c,rows[i:i+POLICY[interval]['horizon']],fee=fee,spread_bps=spread_bps,slippage_bps=slippage_bps)
        if label is None:continue
        result.append({'id':identity([c['id'],VERSION,fee,spread_bps,slippage_bps]),'as_of':as_of,'features':f,'regime':r['label'],'candidate':c,'label':label,'entry_index':i})
    return result

def inspection(store,dataset,lo,hi,run_id,now):
    key=identity([dataset['venue'],dataset['symbol'],dataset['interval']])
    with store.lock,store.db:
        prior=store.db.execute('SELECT COUNT(*) FROM inspections WHERE dataset_key=? AND start<? AND end>?',(key,hi,lo)).fetchone()[0]
        registered=store.db.execute('SELECT * FROM holdouts WHERE start<=? AND end>=? AND registered_at<=?',(lo,hi,lo)).fetchall()
        import json
        registered=[r for r in registered if json.loads(r['body']).get('source_fingerprint')==source_fingerprint()]
        store.db.execute('INSERT INTO inspections(dataset_key,start,end,viewed_at,run_id) VALUES(?,?,?,?,?)',(key,lo,hi,now,run_id))
    return {'previous_overlapping_inspections':prior,'preregistered':bool(registered),'untouched':bool(registered) and prior==0,
            'label':'First registered inspection' if registered and prior==0 else 'Previously inspected / not preregistered; not an untouched holdout'}

def register_holdout(store,start,end,now=None):
    now=time.time() if now is None else now
    if not now<start<end:raise ValueError('A fresh holdout must begin in the future, before labels are available')
    rid=identity([start,end,now])
    with store.lock,store.db:store.db.execute('INSERT INTO holdouts VALUES(?,?,?,?,?)',(rid,start,end,now,encode({'strategy':'trend-pullback-v1','source_fingerprint':source_fingerprint(),'feature_version':'causal-features-v1','forecast_version':VERSION,'criteria':{'minimum_trades':30,'positive_lower_expectancy_bound':True,'max_drawdown':.1}})))
    return rid

def simulate(store,run_id,rows,stream,predictor,rules,*,balance=10,variant='deterministic',fee=.001,spread_bps=10,slippage_bps=10,approvals=None):
    broker=PaperBroker(store,identity([run_id,balance,variant]),balance,fee=str(fee),slippage_bps=slippage_bps)
    by_index={e['entry_index']:e for e in stream};predictions=[];decisions=[]
    if not rows:return broker.metrics(),[],[]
    start_index=min(by_index,default=len(rows)-1)
    broker.mark(rows[start_index]['open'],rows[start_index]['open_ms']/1000)
    for i in range(start_index,len(rows)):
        bar=rows[i];now=bar['open_ms']/1000
        if i in by_index:
            e=by_index[i];pred=predictor.predict(e);predictions.append((pred,e['label']))
            allowed=variant=='original_strategy' or pred['eligible']
            action='BUY' if allowed else 'WAIT';reason='Original fixed-strategy research baseline' if variant=='original_strategy' else '; '.join(pred['abstain_reasons']) or 'Forecast criteria met'
            if variant=='gpt_assisted':
                a=(approvals or {}).get(e['candidate']['id'])
                allowed=allowed and bool(a and a.get('action')=='BUY' and a.get('valid_until',0)>now)
                action='BUY' if allowed else 'WAIT';reason='Validated supplied assessment' if allowed else 'No validated GPT assessment; runtime inference disabled in replay'
            outcome=broker.enter(e['candidate'],reference_book(e['candidate']['symbol'],bar['open'],rules,now,spread_bps),now) if allowed else {'accepted':False,'reason':reason}
            decisions.append({'candidate_id':e['candidate']['id'],'prediction_id':pred['id'],'action':action,'paper_accepted':outcome['accepted'],
                              'reason':outcome.get('reason',reason),'as_of':e['as_of'],'variant':variant,'expected_net_return':pred['expected_net_return'],
                              'actual_target_return':e['label']['net_return'],'memory_ids':[],'fill_reference':None if not outcome['accepted'] else outcome['position']['id']})
        if broker.state['position']:
            # Full reference fills are explicit; rapid/gap/partial behavior is fixture tested separately.
            half=spread_bps/20000
            broker.monitor(float(bar['close'])*(1-half),bar['close_ms']/1000+.001,
                           low=float(bar['low'])*(1-half),high=float(bar['high'])*(1-half),opening=float(bar['open'])*(1-half))
    broker.mark(float(rows[-1]['close'])*(1-spread_bps/20000),rows[-1]['close_ms']/1000+.001)
    metrics=broker.metrics();closed=broker.state['closed'];n=len(closed);values=[float(x['net_pnl']) for x in closed]
    se=1.96*pstdev(values)/math.sqrt(n) if n>=2 else None
    metrics['mean_net_pnl']=mean(values) if n else None
    metrics['approximate_95pct_mean_interval']=[mean(values)-se,mean(values)+se] if se is not None else None
    metrics['adequate_sample']=n>=30
    metrics['candidate_count']=len(stream);metrics['entry_acceptances']=sum(d['paper_accepted'] for d in decisions)
    duration=max(1,(rows[-1]['close_ms']-rows[start_index]['open_ms'])/1000)
    held=sum(x['closed_at']-x['opened_at'] for x in closed)
    if broker.state['position']:held+=rows[-1]['close_ms']/1000-broker.state['position']['opened_at']
    metrics['exposure_fraction']=min(1,held/duration)
    return metrics,predictions,decisions

def evaluate_dataset(store,dataset_id,rows,dataset,*,fee=.001,spread_bps=10,slippage_bps=10,approvals=None):
    fingerprint=source_fingerprint()
    if dataset['interval'] not in POLICY:raise ValueError('First hypothesis evaluates 1m and 5m separately')
    if not dataset['coverage_complete']:raise ValueError('Incomplete dataset; repair gaps before evaluation')
    rules=parse_rules(dataset['metadata']);stream=examples(dataset['symbol'],rows,dataset['interval'],rules['tick'],fee=fee,spread_bps=spread_bps,slippage_bps=slippage_bps)
    now=time.time();run_id=identity([dataset_id,now,fee,spread_bps,slippage_bps]);lo=rows[0]['open_ms']/1000;hi=rows[-1]['close_ms']/1000
    span=hi-lo
    fold_specs=[('walk_forward_1',.35,.5,.5,.65),('walk_forward_2',.5,.65,.65,.8),('chronological_holdout',.6,.8,.8,1.)]
    folds={};last_predictions=[]
    for name,a,b,c,d in fold_specs:
        train,valid,test=chronological(stream,lo+a*span,lo+b*span,lo+c*span)
        test=[x for x in test if x['as_of']<lo+d*span and x['label']['resolved_at']<=lo+d*span]
        predictor=Predictor(train,valid);window=[x for x in rows if lo+c*span<=x['open_ms']/1000<lo+d*span]
        # entry_index refers to the original dataset, so use original rows but restrict end.
        end_index=next((i for i,r in enumerate(rows) if r['open_ms']/1000>=lo+d*span),len(rows))
        results={};all_decisions=[]
        for balance in (5,10):
            results[str(balance)]={}
            for variant in ('original_strategy','deterministic','gpt_assisted'):
                m,p,decisions=simulate(store,run_id+name,rows[:end_index],test,predictor,rules,balance=balance,variant=variant,
                                     fee=fee,spread_bps=spread_bps,slippage_bps=slippage_bps,approvals=approvals)
                results[str(balance)][variant]=m
                if balance==10 and variant=='deterministic':last_predictions=p
                for decision in decisions:
                    decision.update(balance=balance,fold=name,dataset_id=dataset_id,run_id=run_id)
                    store.append('decisions',decision,symbol=dataset['symbol'],as_of=decision['as_of'])
                all_decisions.extend(decisions)
        fold={'split_times':{'train_end':lo+a*span,'validation_end':lo+b*span,'test_start':lo+c*span,'test_end':lo+d*span},
              'purging':'Labels must resolve before the next partition; end-overlap test targets excluded',
              'train_n':len(train),'validation_n':len(valid),'test_n':len(test),'predictor_id':predictor.id,
              'calibration':predictor.calibration,'baseline_brier':predictor.baseline_brier,
              'test_calibration':scorecard([p['probabilities'] for p,l in last_predictions],[l['outcome'] for p,l in last_predictions]),
              'wallets':results}
        if name=='chronological_holdout':fold['inspection']=inspection(store,dataset,lo+c*span,lo+d*span,run_id,now)
        folds[name]=fold
        # Resolved evidence is appended once and remains causally unavailable before resolution.
        for e in test:
            f_id=store.append('feature_snapshots',e['features'],symbol=dataset['symbol'],as_of=e['as_of'],created_at=now,record_id=identity([dataset_id,e['id'],'features']))
            for pred,label in [x for x in last_predictions if x[0]['id']==predictor.predict(e)['id']]:
                body={**pred,'candidate':e['candidate'],'candidate_id':e['candidate']['id'],'dataset_ids':[dataset_id],'feature_id':f_id,'as_of':e['as_of']}
                pid=store.append('predictions',body,symbol=dataset['symbol'],as_of=e['as_of'],created_at=now,record_id=identity([run_id,name,pred['id']]))
                outcome={**label,'prediction_id':pid,'attribution':['Statistical forecast error' if max(pred['probabilities'],key=pred['probabilities'].get)!=label['outcome'] else 'Predicted class matched'],
                         'limitation':'Reference outcome attribution; exchange slippage and GPT contribution require separate forward evidence'}
                oid=store.append('prediction_outcomes',outcome,symbol=dataset['symbol'],as_of=label['resolved_at'],created_at=now)
                remember(store,'observed_fact',{'features':e['features'],'regime':e['regime'],'interval':dataset['interval'],**label},[pid,oid,dataset_id],symbol=dataset['symbol'],as_of=label['resolved_at'])
    if source_fingerprint()!=fingerprint:raise ValueError('Source changed during evaluation; do not release this report')
    report={'id':run_id,'source_fingerprint':fingerprint,'dataset_content_hash':dataset['content_hash'],'dataset_id':dataset_id,'symbol':dataset['symbol'],'interval':dataset['interval'],'created_at':now,
            'strategy':'trend-pullback-v1','forecast_version':VERSION,'candidate_count':len(stream),'folds':folds,
            'costs':{'fee_per_side':fee,'spread_bps':spread_bps,'slippage_bps':slippage_bps},
            'gpt_comparison':'No inference in historical replay. GPT arm abstains unless separately supplied validated assessments; this is not measured GPT improvement.',
            'release':{'live_eligible':False,'status':'NO_VALIDATED_EDGE','requirements':['Adequate independent samples','Calibrated positive net expectancy','Fresh registered holdout','Independent forward paper evidence','Measured GPT comparison','Exchange fault acceptance','Separate live authorization']},
            'limitations':['Reference fills and current filter proxies, not actual historical executions.','Unregistered or reused periods are not untouched.','No automatic parameter search or promotion.','No language model calls were made.']}
    store.append('evaluation_runs',report,symbol=dataset['symbol'],as_of=now,record_id=run_id)
    store.append('hypotheses',{'status':'UNTESTED_PROPOSAL','text':'Collect independent forward observations before considering any timeframe or threshold change.',
                            'evidence_ids':[run_id],'live_promoted':False},symbol=dataset['symbol'],as_of=now)
    return report
