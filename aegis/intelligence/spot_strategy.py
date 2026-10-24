"""Two fixed, unvalidated Spot hypotheses. No fitting, model calls or forming bars."""
from statistics import median
from aegis.domain import D,floor_step
from aegis.strategy import ema
from .store import identity

PAIRS=('BTCUSDT','ETHUSDT','SOLUSDT','DOGEUSDT')
STRATEGIES=('mtf-pullback-v1','breakout-retest-v1')
SPEC={'version':'spot-hypotheses-v1','pairs':PAIRS,'strategies':STRATEGIES,'signal':'1m','trend_filter':'5m',
      'stop':'max(1.5 ATR14, 0.5% entry); reject above 3%','target_gross_R':3,'holding_minutes':30,
      'expiry_seconds':30,'selection':'Fixed pair order, then strategy order; no return-based ranking',
      'risk_accounting':'Received-asset fee; planned marked stop loss; separate 0.10 USDT residual exposure cap',
      'status':'Unvalidated hypotheses; paper only'}


def propose(symbol,one,five,as_of,ask,tick):
    for rows,seconds in ((one,60),(five,300)):
        if len(rows)<65:raise ValueError('Need 65 completed candles on both timeframes')
        if any(b['close_ms']/1000>=as_of for b in rows):raise ValueError('Unfinished candle in signal input')
        if any(b['open_ms']-a['open_ms']!=seconds*1000 for a,b in zip(rows,rows[1:])):raise ValueError('Candle gap; resynchronization required')
        if as_of-rows[-1]['close_ms']/1000>seconds*2:raise ValueError('Completed candle analysis is stale')
    one=one[-150:];five=five[-150:];last,prev=one[-1],one[-2]
    c=[float(r['close']) for r in one];higher=[float(r['close']) for r in five]
    e=ema(c,20);fast=ema(higher,20);slow=ema(higher,50)
    atr=sum(max(float(b['high'])-float(b['low']),abs(float(b['high'])-float(a['close'])),abs(float(b['low'])-float(a['close']))) for a,b in zip(one[-15:-1],one[-14:]))/14
    volume_base=max(median(float(r['volume']) for r in one[-22:-2]),1e-12)
    volume=float(last['volume'])/volume_base
    trend=fast[-1]>slow[-1] and slow[-1]>slow[-6]
    volatility=.05<=atr/c[-1]*100<=3
    bullish=c[-1]>float(last['open'])
    pullback=(abs(c[-2]-e[-2])<=.6*atr or float(prev['low'])<=e[-2]<=float(prev['high']))
    level=max(float(r['high']) for r in one[-23:-3])
    checks={
      STRATEGIES[0]:{'5m uptrend':trend,'volatility':volatility,'pullback':pullback,'rebound':bullish and c[-1]>c[-2] and c[-1]>=e[-1],'volume':volume>=1.2},
      STRATEGIES[1]:{'5m uptrend':trend,'volatility':volatility,'breakout':c[-2]>level,'retest':float(last['low'])<=level+.2*atr and c[-1]>level and bullish,'breakout volume':float(prev['volume'])/volume_base>=1.2}}
    cutoff=last['close_ms']/1000;trend_cutoff=five[-1]['close_ms']/1000
    evidence={'atr14':atr,'atr_pct':atr/c[-1]*100,'volume_ratio':volume,'ema20_1m':e[-1],
              'ema20_5m':fast[-1],'ema50_5m':slow[-1],'resistance_20m':level,'data_cutoff':cutoff,'trend_cutoff':trend_cutoff}
    result=[]
    for strategy in STRATEGIES:
        failed=[k for k,v in checks[strategy].items() if not v]
        reason='Waiting for '+', '.join(failed) if failed else None
        entry=floor_step(ask,tick);distance=max(D(atr)*D('1.5'),entry*D('.005'))
        if distance>entry*D('.03'):reason='Required stop exceeds the 3% strategy cap'
        if as_of>=cutoff+30:reason='Completed-candle entry window expired; wait for the next candle'
        cnd={'eligible':reason is None,'reason':reason or 'Fixed strategy conditions met','symbol':symbol,'interval':'1m',
            'strategy_version':strategy,'id':identity([SPEC['version'],symbol,strategy,cutoff,trend_cutoff]),
            'created_at':cutoff+.001,'data_cutoff':cutoff,'trend_cutoff':trend_cutoff,
            'expires_at':cutoff+30,'holding_minutes':30,'indicators':evidence,'checks':checks[strategy]}
        if cnd['eligible']:
            stop=floor_step(entry-distance,tick);target=floor_step(entry+3*(entry-stop),tick)
            cnd.update(entry=str(entry),stop_loss=str(stop),take_profit=str(target))
        result.append(cnd)
    return result
