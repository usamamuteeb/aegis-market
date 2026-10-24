"""Structured, causal retrieval with conflicting examples and supersession."""
import math
from statistics import mean,pstdev
from .features import KEYS

def remember(store,kind,content,evidence_ids,*,symbol,as_of,status='OBSERVED',expires_at=None,supersedes=None,created_at=None):
    if kind not in ('observed_fact','statistical_finding','hypothesis','gpt_interpretation'):raise ValueError('Unknown knowledge kind')
    return store.append('memory_items',{'kind':kind,'content':content,'evidence_ids':evidence_ids,'status':status,
                        'expires_at':expires_at,'review_at':expires_at,'supersedes':supersedes},symbol=symbol,as_of=as_of,created_at=created_at)

def retrieve(store,features,regime,*,symbol,as_of,limit=4,costs=None):
    costs=costs or {'fee_per_side':.001,'spread_bps':10,'slippage_bps':10}
    rows=store.records('memory_items',symbol=symbol,at=as_of,limit=2000)
    superseded={r['body'].get('supersedes') for r in rows if r['body'].get('supersedes')}
    eligible=[r for r in rows if r['id'] not in superseded and r['body']['status']!='RETRACTED' and
              (r['body'].get('expires_at') is None or r['body']['expires_at']>as_of)]
    analogues=[]
    for row in eligible:
        c=row['body']['content']
        if not isinstance(c,dict) or c.get('regime')!=regime or c.get('interval')!=features['interval']:continue
        if c.get('resolved_at',float('inf'))>as_of or not all(k in c.get('features',{}) for k in KEYS):continue
        if c.get('costs') and c['costs']!=costs:continue
        # Liquidity is matched only when both observations genuinely measured it.
        spread=features.get('spread_bps');past=c['features'].get('spread_bps')
        if (spread is None)!=(past is None) or spread is not None and abs(spread-past)>10:continue
        analogues.append(row)
    raw_count=len(analogues);distinct={}
    for row in analogues:
        c=row['body']['content'];key=(c['interval'],c['features'].get('data_cutoff',c['resolved_at']),c.get('target_version'))
        old=distinct.get(key)
        if old is None or (row['created_at'],row['id'])>(old['created_at'],old['id']):distinct[key]=row
    analogues=list(distinct.values())
    scales={k:max(pstdev([r['body']['content']['features'][k] for r in analogues]),1e-9) if len(analogues)>1 else 1 for k in KEYS}
    ranked=sorted(analogues,key=lambda r:sum(((features[k]-r['body']['content']['features'][k])/scales[k])**2 for k in KEYS))
    selected=ranked[:limit]
    return {'memory_ids':[r['id'] for r in selected],'sample_size':len(analogues),'raw_compatible_records':raw_count,
            'conflicting_outcomes':{label:sum(r['body']['content'].get('outcome')==label for r in analogues) for label in ('TARGET','STOP','TIMEOUT')},
            'examples':[{'id':r['id'],'as_of':r['as_of'],'kind':r['body']['kind'],
                         'outcome':r['body']['content'].get('outcome'),'net_return':r['body']['content'].get('net_return')} for r in selected],
            'normalization':'Only eligible past resolved examples; interval/regime/spread compatibility enforced',
            'limitation':'Repeated replay records are deduplicated and cost scenarios must match. Horizons can still overlap; these are not independent validation or model training.'}
