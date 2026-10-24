"""Display-only public candles and filled forward-paper trades. No signal inputs."""
import math
from .data import INTERVALS


def positive(value):
    try:return math.isfinite(float(value)) and float(value)>0
    except (TypeError,ValueError):return False


def charts(prepared, previews, episodes, position, now,*,wallet_key='paper:forward-deterministic'):
    result=[]
    for (symbol,interval),p in list(prepared.items()):
        rows={r['open_ms']:{**r,'complete':True} for r in p['rows'][-200:]}
        preview=previews.get((symbol,interval))
        if preview and preview['bar']['open_ms']>=max(rows,default=0):
            bar=preview['bar']
            # Even an exchange-closed bar remains a display preview until the
            # completed-candle pipeline accepts it after the local clock boundary.
            if bar['open_ms'] not in rows:
                rows[bar['open_ms']]={**bar,'complete':False,'exchange_closed':preview['exchange_closed']}
            elif not preview['exchange_closed']:
                pass  # An older unfinished update cannot overwrite an accepted bar.
        candles=[rows[k] for k in sorted(rows)][-200:]
        markers=[]
        for record in episodes:
            body=record['body'];kind=body.get('kind')
            if record['symbol']!=symbol or body.get('wallet_key')!=wallet_key or kind not in ('ENTRY','EXIT') or not positive(body.get('filled_qty')):continue
            price=body.get('position',{}).get('entry') if kind=='ENTRY' else body.get('price')
            if not positive(price):continue
            markers.append({'id':record['id'],'at':record['as_of'],'side':'BUY' if kind=='ENTRY' else 'SELL',
                            'price':price,'quantity':body['filled_qty'],'mode':body.get('mode','PAPER'),'wallet_key':wallet_key,'reason':body.get('reason','Entry')})
        levels=None
        if position and position['symbol']==symbol:
            levels={k:position[k] for k in ('entry','stop_loss','take_profit')}
        result.append({'symbol':symbol,'interval':interval,'interval_seconds':INTERVALS[interval],
                       'candles':candles,'markers':sorted(markers,key=lambda m:m['at']),'levels':levels,
                       'preview_received_at':preview['received_at'] if preview else None,
                       'preview_stale':not preview or now-preview['received_at']>10,
                       'source':'Binance public OHLCV; forming candle is display only',
                       'trade_mode':'PAPER','analysis_uses_forming_candle':False})
    return result
