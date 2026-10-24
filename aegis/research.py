"""Read-only historical screening. No exchange credentials or execution adapter."""
import copy
import hashlib
import json
import math
import time
from pathlib import Path
from collections import Counter
from decimal import ROUND_CEILING
from aegis.config import STRATEGY, SYMBOLS
from aegis.domain import D, Blocked, floor_step
from aegis.market import public_get, parse_candles, parse_rules
from aegis.risk import plan, rollover
from aegis.strategy import evaluate
from aegis.qualification import code_fingerprint

DAY=86400

def fetch_history(symbol, start, end, getter=public_get):
    cursor=int(start*1000); rows=[]
    while cursor<int(end*1000):
        batch=getter('klines',symbol=symbol,interval='5m',startTime=cursor,endTime=int(end*1000)-1,limit=1000)
        if not batch: break
        if batch[-1][0]<cursor: raise Blocked('Historical pagination did not advance')
        rows.extend(batch);cursor=int(batch[-1][0])+300000
    candles=parse_candles(rows,int(end*1000))
    if not candles or candles[0]['open_time']>start+300 or candles[-1]['close_time']<end-301:
        raise Blocked('Historical dataset is incomplete')
    return candles

def metrics(trades, initial, max_drawdown):
    vals=[D(t['net_pnl']) for t in trades];wins=[v for v in vals if v>0];losses=[v for v in vals if v<0]
    n=len(vals);p=len(wins)/n if n else 0
    z=1.96
    lower=(p+z*z/(2*n)-z*math.sqrt((p*(1-p)+z*z/(4*n))/n))/(1+z*z/n) if n else None
    return {'trades':n,'net_pnl':str(sum(vals,D(0))),'win_rate_pct':100*p if n else None,
            'win_rate_95pct_lower_bound':100*lower if lower is not None else None,
            'expectancy_usdt':str(sum(vals)/n) if n else None,
            'average_win':str(sum(wins)/len(wins)) if wins else None,
            'average_loss':str(sum(losses)/len(losses)) if losses else None,
            'profit_factor':str(sum(wins)/-sum(losses)) if losses else None,
            'max_drawdown_usdt':str(max_drawdown),'max_drawdown_pct':float(max_drawdown/D(initial)*100)}

def replay(datasets, rules, settings, start, end, fee_rate, spread_bps=10, slippage_bps=None):
    """Signals see only a completed prefix; fills occur at the next bar open."""
    initial=D(settings['allocation']);cash=initial;position=None
    s={'mode':'RUNNING','position':None,'realized':'0','daily_realized':'0','daily_date':'','daily_entries':0,
       'consecutive_losses':0,'daily_halted':False,'last_exit_at':0,'peak_bot_equity':str(initial)}
    cfg={**settings,'slippage_bps':settings['slippage_bps'] if slippage_bps is None else slippage_bps}
    fee=D(fee_rate);half=D(spread_bps)/20000;slip=D(cfg['slippage_bps'])/10000
    trades=[];skips=Counter();peak=initial;drawdown=D(0);curve=[]
    indexes={symbol:{c['open_time']:i for i,c in enumerate(rows)} for symbol,rows in datasets.items()}
    times=sorted({c['open_time'] for rows in datasets.values() for c in rows if start<=c['open_time']<end})
    def close(price,now,reason):
        nonlocal cash,position
        exit_price=D(price)*(1-half)*(1-slip)
        proceeds=D(position['qty'])*exit_price*(1-fee)
        pnl=proceeds-D(position['cost']);cash+=proceeds
        trades.append({**position,'closed_at':now,'exit_price':str(exit_price),'net_pnl':str(pnl),'reason':reason})
        s.update(realized=str(D(s['realized'])+pnl),daily_realized=str(D(s['daily_realized'])+pnl),
                 consecutive_losses=s['consecutive_losses']+1 if pnl<0 else 0,last_exit_at=now,position=None)
        position=None
    for now in times:
        rollover(s,now)
        carried_position = position is not None
        if position:
            symbol=position['symbol'];idx=indexes[symbol].get(now)
            if idx is not None:
                bar=datasets[symbol][idx];stop=D(position['stop_loss']);target=D(position['take_profit'])
                if D(bar['low'])<=stop:
                    close(min(D(bar['open']),stop),now+299.999,'stop (first if both levels touched)')
                elif D(bar['high'])>=target:
                    close(target,now+299.999,'target')
        if not position and not carried_position:
            for symbol in settings['symbols']:
                idx=indexes[symbol].get(now)
                if idx is None or idx<65:continue
                prior=datasets[symbol][max(0,idx-150):idx]
                decision_time=prior[-1]['close_time']+.0001
                mid=D(prior[-1]['close']);ask=mid*(1+half);bid=mid*(1-half)
                result=evaluate(symbol,prior,ask,rules[symbol]['tick'],STRATEGY,decision_time)
                if not result['eligible']:continue
                book={'symbol':symbol,'received_at':decision_time,'bid':str(bid),'ask':str(ask),'mid':str(mid),
                      'spread_bps':spread_bps,'rules':rules[symbol],'avg_mins':5,'avg_price':str(mid),
                      'bids':[],'asks':[]}
                account={'canTrade':True,'synced_at':decision_time,'open_orders':[],
                         'balances':[{'asset':'USDT','free':str(cash),'locked':'0'}]}
                fees={'buy':str(fee),'sell':str(fee),'at':decision_time}
                try:sized=plan(s,cfg,account,result,book,fees,decision_time,False)
                except Blocked as exc:skips[str(exc)]+=1;continue
                entry=D(datasets[symbol][idx]['open'])*(1+half)*(1+slip)
                if abs(entry/D(result['entry'])-1)*10000>30:
                    skips['Next-open entry deviated beyond the fixed tolerance']+=1;continue
                qty=D(sized['qty'])
                if qty>D(prior[-1]['volume'])*D('.001'):
                    skips['Conservative prior-volume capacity limit']+=1;continue
                cost=qty*entry*(1+fee)
                if cost>cash or cost>initial*D(cfg['max_position_pct'])/100:
                    skips['Next-open fill exceeds allocated cash or position cap']+=1;continue
                if not D(result['stop_loss'])<entry<D(result['take_profit']):
                    skips['Next-open fill crossed an exit level']+=1;continue
                cash-=cost
                position={'symbol':symbol,'opened_at':now,'qty':str(qty),'cost':str(cost),
                          'entry_price':str(entry),'stop_loss':result['stop_loss'],'take_profit':result['take_profit']}
                s.update(position=position,daily_entries=s['daily_entries']+1)
                # Apply intrabar exits only after entry; never use this bar to construct the signal.
                bar=datasets[symbol][idx]
                if D(bar['low'])<=D(position['stop_loss']):close(min(D(bar['open']),D(position['stop_loss'])),now+299.999,'entry-bar stop')
                elif D(bar['high'])>=D(position['take_profit']):close(D(position['take_profit']),now+299.999,'entry-bar target')
                break
        equity=cash
        if position:
            idx=indexes[position['symbol']].get(now)
            if idx is not None:equity+=D(position['qty'])*D(datasets[position['symbol']][idx]['close'])*(1-half)*(1-slip)*(1-fee)
        peak=max(peak,equity);drawdown=max(drawdown,peak-equity);s['peak_bot_equity']=str(peak)
        if -D(s['daily_realized'])>=D(cfg['daily_loss_limit']) or equity<=peak*(1-D(cfg['max_drawdown_pct'])/100):s['daily_halted']=True
        if position and D(s['daily_realized'])+equity-(initial+D(s['realized']))<=-D(cfg['daily_loss_limit']):
            close(D(datasets[position['symbol']][indexes[position['symbol']][now]]['close']),now+299.999,'daily marked loss')
        if len(curve)==0 or now-curve[-1]['at']>=3600:curve.append({'at':now,'equity':str(equity)})
    if position:
        rows=[c for c in datasets[position['symbol']] if c['open_time']<end]
        close(D(rows[-1]['close']),end,'end of historical window')
    return {'metrics':metrics(trades,initial,drawdown),'closed_trades':trades,'skipped_candidates':dict(skips),
            'equity_curve':curve,'initial_research_capital':str(initial),'ending_research_capital':str(cash)}

def run_screening(settings, report_path, days=90, progress=None, getter=public_get):
    if days!=90:raise Blocked('The initial experiment is predeclared as 90 calendar days; changing it requires a new experiment')
    source_hash=code_fingerprint()
    cfg=copy.deepcopy(settings);cfg['allocation']=10
    end=int(time.time()//DAY)*DAY;start=end-days*DAY;split=start+60*DAY
    datasets={};rules={};hashes={};reused={}
    data_dir=Path(report_path).parent/'datasets'
    try: previous=json.loads(Path(report_path).read_text(encoding='utf-8'))
    except (OSError,ValueError): previous={}
    for symbol in cfg['symbols']:
        if symbol not in SYMBOLS:raise Blocked('Unsupported historical market')
        if progress:progress('Downloading completed public candles: '+symbol)
        cached=None
        try:
            rows=json.loads((data_dir/(symbol+'.json')).read_text(encoding='utf-8'))
            digest=hashlib.sha256(json.dumps(rows,sort_keys=True).encode()).hexdigest()
            if previous.get('period_start')==start and previous.get('period_end')==end and previous.get('data_hashes',{}).get(symbol)==digest:
                cached=rows
        except (OSError,ValueError,TypeError): pass
        datasets[symbol]=cached if cached is not None else fetch_history(symbol,start-150*300,end,getter)
        reused[symbol]=cached is not None
        rules[symbol]=parse_rules(getter('exchangeInfo',symbol=symbol)['symbols'][0])
        hashes[symbol]=hashlib.sha256(json.dumps(datasets[symbol],sort_keys=True).encode()).hexdigest()
    if progress:progress('Evaluating chronological history and stressed costs; no account orders are sent')
    cost={'fee_per_side':'.001','spread_bps':10,'slippage_bps':cfg['slippage_bps']}
    data_dir=Path(report_path).parent/'datasets'; data_dir.mkdir(parents=True,exist_ok=True)
    profiles={}
    from statistics import mean, pstdev
    for symbol,rows in datasets.items():
        (data_dir/(symbol+'.json')).write_text(json.dumps(rows),encoding='utf-8')
        closes=[c['close'] for c in rows]
        profiles[symbol]={'as_of':end,'last_close':closes[-1],
            'change_7d_pct':(closes[-1]/closes[-2017]-1)*100,
            'change_30d_pct':(closes[-1]/closes[-8641]-1)*100,
            'change_90d_pct':(closes[-1]/closes[-25921]-1)*100,
            'average_7d':mean(closes[-2016:]),'average_30d':mean(closes[-8640:]),
            'realized_daily_volatility_pct':pstdev([(b/a-1)*100 for a,b in zip(closes[-289:-1],closes[-288:])])*math.sqrt(288)}
    blocks={}
    for name,lo,hi in [('first_60_days',start,split),('last_30_days',split,end)]:
        normal=replay(datasets,rules,cfg,lo,hi,D(cost['fee_per_side']))
        stressed=replay(datasets,rules,cfg,lo,hi,D('.002'),spread_bps=20,slippage_bps=max(20,cfg['slippage_bps']*2))
        blocks[name]={'start':lo,'end':hi,'normal_costs':normal,'stressed_costs':stressed}
    if code_fingerprint()!=source_hash: raise Blocked('Source changed during historical screening; report not released')
    report={'created_at':time.time(),'code_fingerprint':source_hash,'settings':cfg,'data_hashes':hashes,
            'exchange_rules':rules,'reused_public_dataset':reused,
            'period_start':start,'period_end':end,'coin_profiles':profiles,'cost_assumptions':cost,'blocks':blocks,'live_eligible':False,
            'limitations':['Screening only: no future-profit claim or high-win-rate guarantee.',
              'Periods may overlap previously examined data; the last block is not claimed to be untouched.',
              'Current exchange filters are proxies for historical filters; actual historical books are unavailable.',
              'Spread, slippage, fills and fees are explicit conservative assumptions, not historical execution records.',
              'Model influence, independent forward behavior and exchange fault acceptance are unvalidated.',
              'Research capital is separate from the connected wallet; no exchange keys or order endpoints are used.']}
    path=Path(report_path);path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_suffix('.tmp');temp.write_text(json.dumps(report,indent=2),encoding='utf-8');temp.replace(path)
    return report
