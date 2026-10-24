"""Signed Binance.com Spot client. No withdrawal, borrowing or futures routes."""
import hashlib
import hmac
import time
from urllib.parse import urlencode
from aegis.domain import Blocked, D
from aegis.transport import call, RemoteError

BASES = {'live': 'https://api.binance.com', 'testnet': 'https://testnet.binance.vision'}
READ = {'account', 'openOrders', 'allOrders', 'myTrades', 'order', 'orderList', 'account/commission'}
WRITE = {('POST', 'order'), ('POST', 'order/test'), ('DELETE', 'order'),
         ('POST', 'orderList/oco'), ('DELETE', 'orderList')}

class Binance:
    def __init__(self, vault):
        self.vault = vault
        self.offset = 0
        self.clock_at = 0
        self.blocked_until = 0

    def credentials(self):
        result = self.vault.read().get('binance')
        if not result:
            raise Blocked('Connect Binance.com')
        return result

    def public(self, path, **params):
        if path not in ('time', 'ticker/bookTicker', 'ticker/price', 'exchangeInfo', 'klines', 'avgPrice', 'depth'):
            raise Blocked('Unsupported public Binance route')
        c = self.credentials()
        return call(BASES[c['environment']] + '/api/v3/' + path + '?' + urlencode(params))

    def request(self, route, method='GET', **params):
        if (method == 'GET' and route not in READ) or (method != 'GET' and (method, route) not in WRITE):
            raise Blocked('Unsupported Spot endpoint')
        if time.time() < self.blocked_until:
            raise Blocked('Binance rate-limit backoff is active')
        c = self.credentials()
        if time.time() - self.clock_at > 60:
            start = time.time()
            server = call(BASES[c['environment']] + '/api/v3/time')['serverTime']
            self.offset = server - (start + time.time()) / 2 * 1000
            self.clock_at = time.time()
        values = {**params, 'timestamp': int(time.time() * 1000 + self.offset), 'recvWindow': 5000}
        encoded = urlencode(values)
        signature = hmac.new(c['secret'].encode(), encoded.encode(), hashlib.sha256).hexdigest()
        query = encoded + '&signature=' + signature
        headers = {'X-MBX-APIKEY': c['key']}
        url = BASES[c['environment']] + '/api/v3/' + route
        body = None
        if method == 'POST':
            body = query.encode()
            headers['Content-Type'] = 'application/x-www-form-urlencoded'
        else:
            url += '?' + query
        try:
            return call(url, method, headers, body)
        except RemoteError as exc:
            if exc.status in (418, 429):
                self.blocked_until = time.time() + max(exc.retry_after, 60)
            raise

    def connect(self, key, secret, environment):
        if environment not in BASES or not isinstance(key, str) or not isinstance(secret, str) or not 20 <= len(key.strip()) <= 512 or not 20 <= len(secret.strip()) <= 2048:
            raise Blocked('Provide valid HMAC API credentials and environment')
        old = self.vault.read().get('binance')
        self.vault.update('binance', {'key': key.strip(), 'secret': secret.strip(), 'environment': environment})
        self.clock_at = 0
        try:
            account = self.request('account', omitZeroBalances='true')
            if account.get('accountType') != 'SPOT':
                raise Blocked('This application requires a Spot account')
            return account
        except Exception:
            self.vault.update('binance', old)
            self.clock_at = 0
            raise

    def status(self):
        c = self.vault.read().get('binance')
        return {'configured': bool(c), 'environment': c['environment'] if c else 'live'}

    def commission(self, symbol):
        data = self.request('account/commission', symbol=symbol)
        if not isinstance(data.get('standardCommission'), dict) or any(k not in data['standardCommission'] for k in ('taker', 'buyer', 'seller')):
            raise Blocked('Account commission response is incomplete')
        buy, sell = D(0), D(0)
        for key in ('standardCommission', 'taxCommission', 'specialCommission'):
            f = data.get(key, {})
            buy += D(f.get('taker', 0)) + D(f.get('buyer', 0))
            sell += D(f.get('taker', 0)) + D(f.get('seller', 0))
        if not 0 <= buy < D('.2') or not 0 <= sell < D('.2'):
            raise Blocked('Account commission is invalid')
        return {'buy': str(buy), 'sell': str(sell), 'raw': data, 'at': time.time()}

def value_assets(account, ticker):
    """USDT mark-to-bid estimate, preserving unpriced assets rather than fabricating zero."""
    rates = {'USDT': D(1)}
    edges = []
    for t in ticker:
        symbol, bid, ask = t['symbol'], D(t['bidPrice']), D(t['askPrice'])
        for quote in ('USDT', 'USDC', 'BTC', 'ETH', 'BNB', 'FDUSD'):
            if symbol.endswith(quote) and len(symbol) > len(quote) and bid > 0 and ask > 0:
                edges.append((symbol[:-len(quote)], quote, bid, ask))
                break
    for _ in range(3):
        for base, quote, bid, ask in edges:
            if quote in rates and base not in rates:
                rates[base] = rates[quote] * bid
            if base in rates and quote not in rates:
                rates[quote] = rates[base] / ask
    rows, total, missing = [], D(0), []
    for a in account.get('balances', []):
        qty = D(a['free']) + D(a['locked'])
        if qty <= 0:
            continue
        rate = rates.get(a['asset'])
        value = qty * rate if rate is not None else None
        if value is None:
            missing.append(a['asset'])
        else:
            total += value
        rows.append({**a, 'total': str(qty), 'value_usdt': str(value) if value is not None else None})
    return {'assets': sorted(rows, key=lambda a: D(a['value_usdt'] or 0), reverse=True),
            'value_usdt': str(total), 'unpriced': missing, 'valuation': 'Spot assets marked to executable bid; conversion is an estimate'}
