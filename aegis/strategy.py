"""Fixed trend/pullback hypothesis. All inputs must be completed candles."""
from statistics import median, pstdev
from aegis.domain import Blocked, D, floor_step, fingerprint

VERSION = 'trend-pullback-v1'


def ema(values, length):
    value = sum(values[:length]) / length
    result = [value] * length
    for x in values[length:]:
        value += 2 / (length + 1) * (x - value)
        result.append(value)
    return result


def evaluate(symbol, candles, ask, tick, settings, now):
    if len(candles) < 65:
        return {'eligible': False, 'reason': '65 completed candles required'}
    if any(c['close_time'] >= now for c in candles):
        raise Blocked('Strategy received an unfinished candle')
    closes = [c['close'] for c in candles]
    e20, e50 = ema(closes, 20), ema(closes, 50)
    trs = [max(c['high'] - c['low'], abs(c['high'] - candles[i - 1]['close']),
               abs(c['low'] - candles[i - 1]['close'])) for i, c in enumerate(candles) if i]
    atr = sum(trs[-14:]) / 14
    last, previous = candles[-1], candles[-2]
    volume_ratio = last['volume'] / max(median([c['volume'] for c in candles[-21:-1]]), 1e-12)
    checks = {
        'trend': e20[-1] > e50[-1] and e50[-1] > e50[-6],
        'pullback': abs(previous['close'] - e20[-2]) <= settings.pullback_atr * atr
                    or previous['low'] <= e20[-2] <= previous['high'],
        'rebound': last['close'] > previous['close'] and last['close'] > last['open'] and last['close'] >= e20[-1],
        'volume': volume_ratio >= settings.min_volume_ratio,
        'volatility': .05 <= atr / last['close'] * 100 <= 3,
    }
    snapshot = {'ema20': e20[-1], 'ema50': e50[-1], 'atr14': atr,
                'atr_pct': atr / last['close'] * 100, 'volume_ratio': volume_ratio,
                'change_1h_pct': (closes[-1] / closes[-13] - 1) * 100,
                'realized_volatility_pct': pstdev([(b / a - 1) * 100 for a, b in zip(closes[-21:-1], closes[-20:])]),
                'checks': checks, 'strategy': VERSION, 'last_candle_time': last['close_time']}
    if not all(checks.values()):
        return {'eligible': False, 'reason': 'Waiting: ' + ', '.join(k for k, v in checks.items() if not v), 'indicators': snapshot}
    entry = floor_step(ask, tick)
    distance = max(D(atr) * D(settings.atr_stop_multiple), entry * D(settings.min_stop_pct) / 100)
    if distance / entry * 100 > D(settings.max_stop_pct):
        return {'eligible': False, 'reason': 'ATR stop distance exceeds strategy limit', 'indicators': snapshot}
    stop = floor_step(entry - distance, tick)
    target = floor_step(entry + (entry - stop) * D(settings.reward_risk), tick)
    cid = fingerprint({'strategy': VERSION, 'symbol': symbol, 'candle': last['close_time']})
    return {'eligible': True, 'id': cid, 'symbol': symbol, 'entry': str(entry), 'stop_loss': str(stop),
            'take_profit': str(target), 'created_at': now, 'expires_at': now + settings.proposal_ttl,
            'reason': 'Rising EMA20/50 trend, pullback then bullish rebound, confirmed volume and ATR',
            'invalidation': ['Stop level touched', 'Proposal expires', 'Entry deviates beyond configured tolerance'],
            'indicators': snapshot}


def position_event(position, book):
    if not position:
        return None
    risk = D(position['entry_price']) - D(position['stop_loss'])
    move = D(book['bid']) - D(position['entry_price'])
    seen = position.get('assessed_events', [])
    if move >= risk and 'one_r' not in seen:
        return 'one_r'
    if move <= -risk / 2 and 'half_r_adverse' not in seen:
        return 'half_r_adverse'
    return None
