from datetime import datetime, timezone
from aegis.domain import D, Blocked, floor_step

def fresh(book, now):
    if not book or not 0 <= now - book['received_at'] <= 10:
        raise Blocked('Market quote is stale or unavailable')
    if not book['rules']['enabled']:
        raise Blocked('Spot market or required native protection is unavailable')

def filters(qty, book, at_price=None):
    r = book['rules']
    if qty <= 0 or qty < D(r['min_qty']) or qty > D(r['max_qty']) or qty % D(r['step']):
        raise Blocked('Quantity violates Binance market/lot filters')
    if any(w not in (0, book['avg_mins']) for w in r['avg_windows']):
        raise Blocked('Unsupported exchange averaging window')
    ref = D(book['avg_price']) if any(w > 0 for w in r['avg_windows']) else D(book['mid'])
    low = min(ref, D(at_price or book['bid']))
    high = max(ref, D(book['ask']))
    if qty * low < D(r['min_notional']):
        raise Blocked(f'{book["symbol"]}: rounded order value {qty * low:.4f} USDT is below the exchange minimum {D(r["min_notional"]):.2f} USDT')
    if r['max_notional'] and qty * high > D(r['max_notional']):
        raise Blocked('Exchange maximum notional exceeded')

def protective_filters(qty, stop, target, book):
    levels = [D(stop), D(target)]
    for f in book['rules'].get('raw_filters', []):
        kind = f['filterType']
        if kind == 'PRICE_FILTER':
            for level in levels:
                if (D(f['minPrice']) > 0 and level < D(f['minPrice'])) or (D(f['maxPrice']) > 0 and level > D(f['maxPrice'])) or level % D(f['tickSize']):
                    raise Blocked('Protective price violates exchange price filters')
        if kind in ('PERCENT_PRICE', 'PERCENT_PRICE_BY_SIDE'):
            window = int(f.get('avgPriceMins', 0))
            if window not in (0, book['avg_mins']): raise Blocked('Unsupported protective price averaging window')
            ref = D(book['avg_price'] if window else book['mid'])
            low = D(f['bidMultiplierDown'] if kind == 'PERCENT_PRICE_BY_SIDE' else f['multiplierDown'])
            high = D(f['bidMultiplierUp'] if kind == 'PERCENT_PRICE_BY_SIDE' else f['multiplierUp'])
            # Both protective legs SELL the base asset: ask-side multipliers apply.
            if kind == 'PERCENT_PRICE_BY_SIDE': low, high = D(f['askMultiplierDown']), D(f['askMultiplierUp'])
            if any(not ref * low <= p <= ref * high for p in levels): raise Blocked('Protective level exceeds exchange percent-price limits')
        if kind in ('MIN_NOTIONAL', 'NOTIONAL'):
            if any(qty * p < D(f['minNotional']) for p in levels): raise Blocked('Protective order below minimum notional')
            if kind == 'NOTIONAL' and D(f['maxNotional']) > 0 and any(qty * p > D(f['maxNotional']) for p in levels): raise Blocked('Protective order above maximum notional')
        if kind in ('MAX_NUM_ORDERS', 'MAX_NUM_ALGO_ORDERS'):
            maximum = int(f.get('maxNumOrders', f.get('maxNumAlgoOrders', 0)))
            needed = 2 if kind == 'MAX_NUM_ORDERS' else 1
            if maximum and maximum < needed: raise Blocked('Exchange order capacity cannot support native protection')

def plan(state, settings, account, candidate, book, fee, now, has_pending):
    fresh(book, now)
    if state['mode'] != 'RUNNING' or state['position'] or has_pending:
        raise Blocked('Entries paused, a position exists, or an order is unresolved')
    if not account or now - account['synced_at'] > 20 or not account.get('canTrade'):
        raise Blocked('A fresh trade-enabled Spot account is required')
    if account.get('open_orders'):
        raise Blocked('Resolve existing account orders before opening an automated position')
    if not settings['allocation']:
        raise Blocked('Set a real trading allocation first')
    if state['daily_halted'] or -D(state['daily_realized']) >= D(settings['daily_loss_limit']):
        raise Blocked('Daily loss limit reached')
    if state['consecutive_losses'] >= settings['max_consecutive_losses']:
        raise Blocked('Consecutive loss limit reached')
    if state['daily_entries'] >= settings['max_trades_per_day']:
        raise Blocked('Daily trade limit reached')
    if now - state['last_exit_at'] < 900:
        raise Blocked('Post-exit cooldown')
    if book['spread_bps'] > settings['max_spread_bps'] or now >= candidate['expires_at']:
        raise Blocked('Spread excessive or candidate expired')
    entry, stop, target = D(candidate['entry']), D(candidate['stop_loss']), D(candidate['take_profit'])
    if not 0 < stop < entry < target or any(p % D(book['rules']['tick']) for p in (entry, stop, target)):
        raise Blocked('Invalid strategy price levels')
    if abs(D(book['ask']) / entry - 1) * 10000 > 30:
        raise Blocked('Entry moved outside proposal validity')
    if not fee or now - fee['at'] > 300:
        raise Blocked('Account-specific commission is unavailable or stale')
    free = next((D(b['free']) for b in account['balances'] if b['asset'] == 'USDT'), D(0))
    allocation = D(settings['allocation'])
    capital = min(allocation, allocation + D(state['realized']))
    if capital <= 0:
        raise Blocked('Trading allocation exhausted')
    if state.get('peak_bot_equity') and capital <= D(state['peak_bot_equity']) * (1 - D(settings['max_drawdown_pct']) / 100):
        raise Blocked('Trading drawdown limit reached')
    buy_fee, sell_fee = D(fee['buy']), D(fee['sell'])
    slip = D(settings['slippage_bps']) / 10000
    worst = D(book['ask']) * (1 + slip)
    stop_net = stop * (1 - slip) * (1 - sell_fee)
    risk_unit = worst * (1 + buy_fee) - stop_net
    reward_unit = target * (1 - sell_fee) - worst * (1 + buy_fee)
    if risk_unit <= 0 or reward_unit < risk_unit:
        raise Blocked('Net reward does not cover stop risk and commissions')
    max_cost = min(free / (1 + buy_fee), capital * D(settings['max_position_pct']) / 100)
    qty = floor_step(min(max_cost / worst, min(D(settings['risk_per_trade']), max(D(0), D(settings['daily_loss_limit']) + D(state['daily_realized']))) / risk_unit), book['rules']['step'])
    filters(qty, book, stop_net)
    sellable = floor_step(qty * (1 - buy_fee), book['rules']['step'])
    filters(sellable, book, stop_net)
    protective_filters(sellable, stop, target, book)
    for f in book['rules'].get('raw_filters', []):
        if f['filterType'] == 'MAX_POSITION':
            held = next((D(b['free']) + D(b['locked']) for b in account['balances'] if b['asset'] == candidate['symbol'][:-4]), D(0))
            if held + qty > D(f['maxPosition']): raise Blocked('Exchange maximum asset position exceeded')
    return {'qty': str(qty), 'risk': str(qty * risk_unit), 'max_quote': str(max_cost), 'entry': str(entry),
            'stop_loss': str(stop), 'take_profit': str(target)}

def rollover(state, now):
    day = datetime.fromtimestamp(now, timezone.utc).date().isoformat()
    if day != state['daily_date']:
        state.update(daily_date=day, daily_realized='0', daily_entries=0, consecutive_losses=0, daily_halted=False)
