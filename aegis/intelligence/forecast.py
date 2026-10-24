"""Predefined triple-barrier target and calibrated, shrinkage histogram predictor.

No language model trains probabilities. Fitting/calibration never see holdout labels.
"""
import math
from statistics import mean,pstdev
from .features import KEYS
from .store import identity

OUTCOMES=('TARGET','STOP','TIMEOUT')
VERSION='regime-histogram-shrinkage-v1'
TARGET_VERSION='next-open-barriers-stop-first-v1'

def resolve(candidate,future,*,fee=.001,spread_bps=10,slippage_bps=10):
    horizon=candidate['horizon_bars']
    if not future:return None
    expected_step=60 if candidate['interval']=='1m' else 300
    if any((b['open_ms']-a['open_ms'])!=expected_step*1000 for a,b in zip(future[:horizon],future[1:horizon])):return None
    half=spread_bps/20000;slip=slippage_bps/10000
    entry=float(future[0]['open'])*(1+half)*(1+slip);stop=float(candidate['stop_loss']);target=float(candidate['take_profit'])
    if not stop<entry<target:return None
    outcome=None;exit_price=None;bar=None
    for bar in future[:horizon]:
        low,high,opening=float(bar['low'])*(1-half),float(bar['high'])*(1-half),float(bar['open'])*(1-half)
        if low<=stop:outcome='STOP';exit_price=min(opening,stop)*(1-slip);break
        if high>=target:outcome='TARGET';exit_price=target*(1-slip);break
    if outcome is None:
        if len(future)<horizon:return None
        bar=future[horizon-1];outcome='TIMEOUT';exit_price=float(bar['close'])*(1-half)*(1-slip)
    net_return=exit_price*(1-fee)/(entry*(1+fee))-1
    return {'outcome':outcome,'entry':entry,'exit':exit_price,'net_return':net_return,
            'resolved_at':bar['close_ms']/1000+.001,'entry_at':future[0]['open_ms']/1000,
            'horizon_bars':horizon,'target_version':TARGET_VERSION,
            'costs':{'fee_per_side':fee,'spread_bps':spread_bps,'slippage_bps':slippage_bps},
            'assumptions':'Next bar open, conservative stop-first ambiguous bar, adverse gap/exit costs, full reference fills; executable partial fills handled separately'}

def brier(probabilities,labels):
    if not labels:return None
    return mean(sum((p[k]-float(label==k))**2 for k in OUTCOMES) for p,label in zip(probabilities,labels))

def scorecard(probabilities,labels):
    bins=[]
    for lo,hi in ((0,.33),(.33,.66),(.66,1.0000001)):
        selected=[(p['TARGET'],label=='TARGET') for p,label in zip(probabilities,labels) if lo<=p['TARGET']<hi]
        bins.append({'range':[lo,min(hi,1)],'n':len(selected),'predicted':mean(x for x,y in selected) if selected else None,
                     'observed':mean(y for x,y in selected) if selected else None})
    return {'n':len(labels),'multiclass_brier':brier(probabilities,labels),'target_reliability_bins':bins}

def group(row):
    f=row['features'];return (row['regime'],int(f['ema_gap']>0),int(f['volatility']>.002))

class Predictor:
    def __init__(self,train,validation,*,min_train=30,min_validation=20):
        self.train=train;self.validation=validation;self.alpha=0.;self.version=VERSION
        self.ready=len(train)>=min_train and len(validation)>=min_validation
        self.baseline={k:(sum(r['label']['outcome']==k for r in train)+1)/(len(train)+3) for k in OUTCOMES}
        self.bounds={k:(min((r['features'][k] for r in train),default=0),max((r['features'][k] for r in train),default=0)) for k in KEYS}
        self.id=identity([VERSION,[r['id'] for r in train],[r['id'] for r in validation]])
        if self.ready:
            options=[]
            for alpha in (0.,.25,.5,.75,1.):
                predictions=[self.probabilities(r,alpha) for r in validation]
                options.append((brier(predictions,[r['label']['outcome'] for r in validation]),alpha))
            self.alpha=min(options)[1]
        probabilities=[self.probabilities(r) for r in validation]
        labels=[r['label']['outcome'] for r in validation]
        self.calibration=scorecard(probabilities,labels)
        self.baseline_brier=brier([self.baseline for r in validation],labels)
        self.calibration_ok=self.ready and self.calibration['multiclass_brier']<=.8 and self.calibration['multiclass_brier']<=self.baseline_brier+1e-12

    def probabilities(self,row,alpha=None):
        compatible=[r for r in self.train if group(r)==group(row)]
        local={k:(sum(r['label']['outcome']==k for r in compatible)+5*self.baseline[k])/(len(compatible)+5) for k in OUTCOMES}
        a=self.alpha if alpha is None else alpha
        return {k:(1-a)*self.baseline[k]+a*local[k] for k in OUTCOMES}

    def predict(self,row):
        compatible=[r for r in self.train if group(r)==group(row)]
        probabilities=self.probabilities(row)
        means={k:mean([r['label']['net_return'] for r in compatible if r['label']['outcome']==k] or
                      [r['label']['net_return'] for r in self.train if r['label']['outcome']==k] or [0]) for k in OUTCOMES}
        expectancy=sum(probabilities[k]*means[k] for k in OUTCOMES)
        returns=[r['label']['net_return'] for r in compatible];n=len(returns)
        uncertainty=1.96*pstdev(returns)/math.sqrt(n) if n>=2 else None
        interval=[expectancy-uncertainty,expectancy+uncertainty] if uncertainty is not None else None
        ood=[]
        for k,(low,high) in self.bounds.items():
            margin=max((high-low)*.2,1e-9)
            if not low-margin<=row['features'][k]<=high+margin:ood.append(k)
        reasons=[]
        if not self.ready:reasons.append('Insufficient chronological training/calibration samples')
        if not self.calibration_ok:reasons.append('Calibration has not met declared validation criteria')
        if n<20:reasons.append('Fewer than 20 compatible resolved analogues')
        if ood:reasons.append('Out-of-distribution features: '+', '.join(ood))
        if interval is None or interval[0]<=0:reasons.append('Positive net expectancy lower bound is unsupported')
        return {'id':identity([self.id,row['id']]),'model_id':self.id,'version':VERSION,'probabilities':probabilities,
                'outcome_mean_returns':means,'expected_net_return':expectancy,'approximate_95pct_interval':interval,
                'sample_size':n,'training_n':len(self.train),'validation_n':len(self.validation),
                'training_cutoff':max((r['label']['resolved_at'] for r in self.train),default=None),
                'calibration_cutoff':max((r['label']['resolved_at'] for r in self.validation),default=None),
                'calibration':self.calibration,'baseline_brier':self.baseline_brier,'calibration_weight':self.alpha,
                'eligible':not reasons,'abstain_reasons':reasons,'target_version':TARGET_VERSION,
                'horizon_bars':row['candidate']['horizon_bars'],'interval':row['candidate']['interval'],
                'entry_assumption':'Next observable open with stated costs; approval is not an executable quantity',
                'limitations':['Normal-approximation interval is descriptive, not a coverage guarantee.','Sparse regimes abstain.','Historical full reference fills are not actual exchange execution.']}

def chronological(examples,train_end,validation_end,test_start):
    train=[r for r in examples if r['as_of']<train_end and r['label']['resolved_at']<train_end]
    validation=[r for r in examples if train_end<=r['as_of']<validation_end and r['label']['resolved_at']<validation_end]
    test=[r for r in examples if r['as_of']>=test_start]
    return train,validation,test
