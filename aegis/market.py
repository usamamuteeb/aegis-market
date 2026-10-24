"""Only unsigned public GET routes are reachable through this adapter."""
import json
import math
import time
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from functools import reduce
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from aegis.domain import Blocked, D

BASE = 'https://data-api.binance.vision'
ROUTES = {'exchangeInfo', 'depth', 'klines', 'avgPrice', 'time', 'ticker/24hr'}


def public_get(route, **params):
    if route not in ROUTES:
        raise Blocked('Only allowlisted public Binance market-data routes are supported')
    request = Request(BASE + '/api/v3/' + route + '?' + urlencode(params),
                      headers={'User-Agent': 'Aegis/3.0'})
    with urlopen(request, timeout=5) as response:
        return json.load(response)


def combined_step(steps):
    vals = [D(s) for s in steps if D(s) > 0]
    if not vals:
        raise Blocked('Missing quantity step')
    scale = 10 ** max(max(0, -v.as_tuple().exponent) for v in vals)
    return str(Decimal(reduce(math.lcm, [int(v * scale) for v in vals])) / scale)


def parse_rules(product):
    filters = {f['filterType']: f for f in product['filters']}
    lot = filters['LOT_SIZE']
    market_lot = filters.get('MARKET_LOT_SIZE', {})
    minimums, maximums, windows = [], [], []
    if 'MIN_NOTIONAL' in filters:
        f = filters['MIN_NOTIONAL']
        if f.get('applyToMarket', False):
            minimums.append(D(f['minNotional']))
            windows.append(int(f.get('avgPriceMins', 0)))
    if 'NOTIONAL' in filters:
        f = filters['NOTIONAL']
        if f.get('applyMinToMarket', False):
            minimums.append(D(f['minNotional']))
            windows.append(int(f.get('avgPriceMins', 0)))
        if f.get('applyMaxToMarket', False):
            maximums.append(D(f['maxNotional']))
            windows.append(int(f.get('avgPriceMins', 0)))
    max_qty = [D(f['maxQty']) for f in (lot, market_lot) if D(f.get('maxQty', 0)) > 0]
    return {'step': combined_step([lot['stepSize'], market_lot.get('stepSize', 0)]),
            'min_qty': str(max(D(lot['minQty']), D(market_lot.get('minQty', 0)))),
            'max_qty': str(min(max_qty)), 'tick': filters['PRICE_FILTER']['tickSize'],
            'min_notional': str(max(minimums, default=D(0))),
            'max_notional': str(min(maximums)) if maximums else None,
            'avg_windows': sorted(set(windows)), 'raw_filters': product['filters'],
            'enabled': product['status'] == 'TRADING' and product.get('isSpotTradingAllowed', False)
                       and product.get('ocoAllowed', False)
                       and {'MARKET', 'STOP_LOSS', 'LIMIT_MAKER'}.issubset(product.get('orderTypes', [])),
            'captured_at': time.time()}


def parse_candles(rows, cutoff_ms):
    candles = [{'open_time': int(r[0]) / 1000, 'close_time': int(r[6]) / 1000,
                'open': float(r[1]), 'high': float(r[2]), 'low': float(r[3]),
                'close': float(r[4]), 'volume': float(r[5]), 'quote_volume': float(r[7])}
               for r in rows if int(r[6]) < cutoff_ms]
    candles.sort(key=lambda c: c['open_time'])
    for i, candle in enumerate(candles):
        if any(not math.isfinite(candle[k]) or candle[k] <= 0 for k in ('open', 'high', 'low', 'close')):
            raise Blocked('Invalid candle prices')
        if candle['volume'] < 0 or candle['low'] > min(candle['open'], candle['close']) or candle['high'] < max(candle['open'], candle['close']):
            raise Blocked('Invalid candle range or volume')
        if i and candle['open_time'] - candles[i - 1]['open_time'] != 300:
            raise Blocked('Missing or duplicate completed candles')
    return candles


class BinanceFeed:
    def __init__(self, getter=public_get):
        self.cache = {}
        self.get = getter

    def fetch(self, symbol):
        now = time.time()
        cache = self.cache.setdefault(symbol, {})
        if now - cache.get('rules_at', 0) > 3600:
            product = self.get('exchangeInfo', symbol=symbol)['symbols'][0]
            cache.update(rules=parse_rules(product), rules_at=now)
        if now - cache.get('candles_at', 0) > 15:
            rows = self.get('klines', symbol=symbol, interval='5m', limit=150)
            average = self.get('avgPrice', symbol=symbol)
            cache.update(candles=parse_candles(rows, int(time.time() * 1000)), candles_at=time.time(), average=average)
        if len(cache['candles']) < 65 or time.time() - cache['candles'][-1]['close_time'] > 620:
            raise Blocked('Completed candle feed stale or too short')
        start = time.time()
        book = self.get('depth', symbol=symbol, limit=100)
        received = time.time()
        if received - start > 5:
            raise Blocked('Book request latency too high')
        bids, asks = book['bids'], book['asks']
        if not bids or not asks or any(D(p) <= 0 or D(q) <= 0 for p, q in bids + asks):
            raise Blocked('Invalid market book')
        bid, ask = float(bids[0][0]), float(asks[0][0])
        if bid > ask:
            raise Blocked('Crossed market book')
        return {'symbol': symbol, 'bid': bid, 'ask': ask, 'mid': (bid + ask) / 2,
                'spread_bps': (ask - bid) / ((ask + bid) / 2) * 10000,
                'received_at': received, 'request_seconds': received - start,
                'book_id': str(book['lastUpdateId']), 'bids': bids, 'asks': asks,
                'rules': cache['rules'], 'avg_price': cache['average']['price'],
                'avg_mins': cache['average']['mins'], 'avg_received_at': cache['candles_at'],
                'candles': cache['candles'], 'source': 'Binance public market-data-only REST',
                'freshness_basis': 'HTTP receipt time; public REST depth has no exchange timestamp'}

    def fetch_all(self, symbols):
        markets, errors = {}, {}
        with ThreadPoolExecutor(max_workers=len(symbols)) as pool:
            tasks = {s: pool.submit(self.fetch, s) for s in symbols}
            for symbol, task in tasks.items():
                try:
                    markets[symbol] = task.result()
                except Exception as exc:
                    errors[symbol] = type(exc).__name__ + ': public data unavailable; execution blocked'
        return markets, errors
