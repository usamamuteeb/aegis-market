"""Small causal feature set, defined regimes and versioned coin profiles."""
import math
from statistics import mean,pstdev,median
from .store import identity
from .data import INTERVALS,numeric
from aegis.strategy import ema,evaluate
from aegis.config import StrategySettings

FEATURE_VERSION='causal-features-v1'
REGIME_VERSION='ema-volatility-regime-v1'
KEYS=('return_12','ema_gap','volatility','volume_ratio','support_distance','resistance_distance')
POLICY={'1m':{'horizon':60,'ttl':30,'max_feed_age':3,'max_deviation_bps':15},
        '5m':{'horizon':12,'ttl':90,'max_feed_age':10,'max_deviation_bps':30}}

def features(rows,interval,as_of,benchmark=None,spread_bps=None):
    bars=[r for r in rows if r['close_ms']/1000<as_of][-150:]
    if len(bars)<65:raise ValueError('65 completed consecutive candles required')
    step=INTERVALS[interval]*1000
    if any(b['open_ms']-a['open_ms']!=step for a,b in zip(bars,bars[1:])):raise ValueError('Feature window contains a gap')
    if as_of-bars[-1]['close_ms']/1000>INTERVALS[interval]*2:raise ValueError('Feature window is stale')
    closes=[float(b['close']) for b in bars];returns=[b/a-1 for a,b in zip(closes,closes[1:])]
    fast,slow=ema(closes,20),ema(closes,50);vol=pstdev(returns[-24:]);vol_base=median([abs(x) for x in returns[:-24]])
    support=min(float(b['low']) for b in bars[-20:]);resistance=max(float(b['high']) for b in bars[-20:])
    body={'version':FEATURE_VERSION,'as_of':as_of,'data_cutoff':bars[-1]['close_ms']/1000,'interval':interval,
          'return_12':closes[-1]/closes[-13]-1,'ema_gap':fast[-1]/slow[-1]-1,'trend_slope':slow[-1]/slow[-6]-1,
          'volatility':vol,'volume_ratio':float(bars[-1]['volume'])/max(median(float(b['volume']) for b in bars[-21:-1]),1e-12),
          'support_distance':closes[-1]/support-1,'resistance_distance':resistance/closes[-1]-1,
          'support':support,'resistance':resistance,'spread_bps':spread_bps,'benchmark_correlation':None,
          'abnormal_activity':vol>max(vol_base*4,.002),'liquidity_known':spread_bps is not None}
    if benchmark:
        refs={b['close_ms']:float(b['close']) for b in benchmark if b['close_ms']/1000<as_of}
        aligned=[(float(b['close']),refs[b['close_ms']]) for b in bars if b['close_ms'] in refs]
        if len(aligned)>=30:
            x=[b[0]/a[0]-1 for a,b in zip(aligned,aligned[1:])];y=[b[1]/a[1]-1 for a,b in zip(aligned,aligned[1:])]
            if pstdev(x)*pstdev(y)>0:body['benchmark_correlation']=mean([(a-mean(x))*(b-mean(y)) for a,b in zip(x,y)])/(pstdev(x)*pstdev(y))
    label='unusually_volatile' if body['abnormal_activity'] else 'trending' if abs(body['ema_gap'])>.002 and abs(body['trend_slope'])>.0005 else 'ranging'
    regime={'label':label,'interval':interval,'version':REGIME_VERSION,'definition':'volatile if recent sigma > max(4× prior median absolute return, 0.2%); trending if |EMA20/50 gap|>0.2% and |EMA50 5-bar slope|>0.05%; otherwise ranging',
            'uncertainty':'Threshold rule, not a probability; boundary changes can alter the label','as_of':as_of}
    return body,regime

def candidate(symbol,rows,interval,as_of,ask,tick):
    policy=POLICY[interval]
    rows=[r for r in rows if r['close_ms']/1000<as_of]
    c=evaluate(symbol,numeric([r for r in rows if r['close_ms']/1000<as_of][-150:]),ask,tick,StrategySettings(proposal_ttl=policy['ttl']),as_of)
    c.update(interval=interval,strategy_version='trend-pullback-v1@'+interval,horizon_bars=policy['horizon'])
    if c['eligible']:c['id']=identity([c['id'],interval]);c['data_cutoff']=rows[-1]['close_ms']/1000
    return c

def profile(dataset_id,dataset,rows,benchmark=None):
    if not rows:raise ValueError('Cannot profile an empty dataset')
    prices=[float(r['close']) for r in rows];changes=[b/a-1 for a,b in zip(prices,prices[1:])]
    peak=prices[0];dd=0
    for p in prices:peak=max(peak,p);dd=max(dd,1-p/peak)
    sample=[]
    for i in range(65,len(rows),max(1,len(rows)//120)):
        try:_,r=features(rows[:i],dataset['interval'],rows[i-1]['close_ms']/1000+.001);sample.append(r['label'])
        except ValueError:pass
    correlation=None
    if benchmark:
        refs={r['close_ms']:float(r['close']) for r in benchmark}
        aligned=[(float(r['close']),refs[r['close_ms']]) for r in rows if r['close_ms'] in refs]
        if len(aligned)>=30:
            x=[b[0]/a[0]-1 for a,b in zip(aligned,aligned[1:])];y=[b[1]/a[1]-1 for a,b in zip(aligned,aligned[1:])]
            mx,my=mean(x),mean(y)
            if pstdev(x)*pstdev(y)>0:correlation=mean([(a-mx)*(b-my) for a,b in zip(x,y)])/(pstdev(x)*pstdev(y))
    return {'version':'coin-profile-v1','symbol':dataset['symbol'],'venue':dataset['venue'],'base_asset':dataset['base_asset'],
            'quote_asset':dataset['quote_asset'],'dataset_ids':[dataset_id],'coverage':{k:dataset[k] for k in ('bars','first_open','last_close','coverage_complete','missing_bars')},
            'price_return':prices[-1]/prices[0]-1,'max_drawdown':dd,'bar_volatility':pstdev(changes) if len(changes)>1 else None,
            'median_quote_volume':median(float(r['volume'])*float(r['close']) for r in rows),
            'sampled_regime_counts':{k:sample.count(k) for k in sorted(set(sample))},'benchmark_correlation':correlation,
            'limitations':dataset['limitations']+['Quote volume is a liquidity proxy, not executable historical depth.','Regime counts sample fixed completed windows.']}
