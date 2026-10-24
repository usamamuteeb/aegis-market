"""Persist-before-send order intents. An uncertain POST is never blindly retried."""
import copy
import time
from aegis.domain import Blocked, D, fingerprint, floor_step
from aegis.risk import filters, fresh, protective_filters
from aegis.transport import RemoteError

FINAL = {'FILLED', 'CANCELED', 'EXPIRED', 'REJECTED', 'EXPIRED_IN_MATCH'}

class Broker:
    def __init__(self, api, store):
        self.api, self.store = api, store

    def submit(self, key, kind, route, method='POST', generation=None, **params):
        with self.store.lock:
            if self.store.intent(key):
                return self.store.intent(key)
            s = copy.deepcopy(self.store.state)
            if kind == 'ENTRY' and (s['mode'] != 'RUNNING' or generation != s['generation']):
                raise Blocked('Entry invalidated by Pause or Stop')
            intent = {'id': key, 'kind': kind, 'route': route, 'method': method, 'params': params,
                      'status': 'SENDING', 'created_at': time.time(), 'response': None}
            self.store.save(s, ('order', {'message': kind + ' request committed', 'intent': key}), intent=intent)
        try:
            response = self.api.request(route, method, **params)
            intent.update(status='ACCEPTED', response=response)
        except RemoteError as exc:
            intent.update(status='UNKNOWN' if exc.unknown else 'REJECTED', error=str(exc))
        except Exception:
            # Once committed, a local transport exception cannot prove that Binance did not receive it.
            intent.update(status='UNKNOWN', error='Request status unknown; reconciliation required')
        with self.store.lock:
            self.store.save(copy.deepcopy(self.store.state), ('order', {'message': kind + ': ' + intent['status'], 'intent': key}), intent=intent)
        return intent

    def unresolved(self):
        return self.store.intents({'SENDING', 'UNKNOWN'})

    def reconcile(self):
        for intent in self.store.intents({'SENDING', 'UNKNOWN', 'ACCEPTED'}):
            kind, p = intent['kind'], intent['params']
            if intent['status'] == 'ACCEPTED' and kind not in ('ENTRY', 'EXIT', 'CANCEL_ENTRY'):
                continue
            if intent['status'] == 'ACCEPTED' and (intent.get('response') or {}).get('status') in FINAL:
                continue
            try:
                if kind in ('PROTECTION', 'CANCEL'):
                    query = {'orderListId': p['orderListId']} if kind == 'CANCEL' else {'origClientOrderId': p['listClientOrderId']}
                    response = self.api.request('orderList', **query)
                    if kind == 'CANCEL' and response.get('listOrderStatus') != 'ALL_DONE':
                        continue
                else:
                    response = self.api.request('order', symbol=p['symbol'], origClientOrderId=p.get('newClientOrderId', p.get('origClientOrderId')))
                intent.update(status='ACCEPTED', response=response, reconciled_at=time.time())
                with self.store.lock:
                    self.store.save(copy.deepcopy(self.store.state), ('reconciled', {'message': kind + ' acknowledged by Binance', 'intent': intent['id']}), intent=intent)
            except RemoteError as exc:
                if exc.code in (-2013, -2026):
                    # Absence from one read is not proof an uncertain mutation failed.
                    continue
            except Exception:
                continue

    def entry(self, candidate, sized, generation):
        key = 'entry:' + candidate['id']
        tag = 'agE' + fingerprint(key)[:26]
        return self.submit(key, 'ENTRY', 'order', generation=generation,
                           symbol=candidate['symbol'], side='BUY', type='MARKET', quantity=sized['qty'],
                           newClientOrderId=tag, newOrderRespType='FULL')

    def cancel_entry(self, entry):
        return self.submit('cancel-entry:' + entry['id'], 'CANCEL_ENTRY', 'order', method='DELETE',
                           symbol=entry['params']['symbol'], origClientOrderId=entry['params']['newClientOrderId'])

    def protection(self, position, book, account=None):
        fresh(book, time.time())
        qty = floor_step(position['remaining_qty'], book['rules']['step'])
        filters(qty, book, position['stop_loss'])
        protective_filters(qty, position['stop_loss'], position['take_profit'], book)
        if account and qty > self.owned_available(position, account):
            raise Blocked('Managed quantity is unavailable without using pre-existing holdings')
        if not D(position['stop_loss']) < D(book['bid']) < D(position['take_profit']):
            raise Blocked('Exit level already reached')
        revision = position.get('protection_revision', 0)
        key = 'protect:' + position['entry_intent'] + ':' + str(revision)
        tag = 'agP' + fingerprint(key)[:26]
        return self.submit(key, 'PROTECTION', 'orderList/oco', symbol=position['symbol'], side='SELL', quantity=str(qty),
                           listClientOrderId=tag, aboveType='LIMIT_MAKER', abovePrice=position['take_profit'],
                           aboveClientOrderId='agT' + fingerprint(key)[:26],
                           belowType='STOP_LOSS', belowStopPrice=position['stop_loss'],
                           belowClientOrderId='agS' + fingerprint(key)[:26], newOrderRespType='FULL')

    def cancel_protection(self, position):
        oid = position.get('protection_order_list_id')
        if oid is None:
            return None
        key = 'cancel:' + str(oid)
        return self.submit(key, 'CANCEL', 'orderList', method='DELETE', symbol=position['symbol'], orderListId=oid)

    def owned_available(self, position, account):
        asset = next((b for b in account['balances'] if b['asset'] == position['symbol'][:-4]), {'free': '0', 'locked': '0'})
        total = D(asset['free']) + D(asset['locked'])
        owned = max(D(0), total - D(position.get('unmanaged_base_floor', 0)))
        return min(D(asset['free']), owned)

    def exit(self, position, book, account):
        fresh(book, time.time())
        base = position['symbol'][:-4]
        free = self.owned_available(position, account)
        qty = floor_step(min(D(position['remaining_qty']), free), book['rules']['step'])
        filters(qty, book)
        key = 'exit:' + position['entry_intent'] + ':' + position['remaining_qty']
        return self.submit(key, 'EXIT', 'order', symbol=position['symbol'], side='SELL', type='MARKET', quantity=str(qty),
                           newClientOrderId='agX' + fingerprint(key)[:26], newOrderRespType='FULL')

def fill_cost(trades, symbol):
    """Exact quote/base commission conversion at the fill price; preserve other fee assets as unknown."""
    base = symbol[:-4]
    gross, fee, qty, base_fee = D(0), D(0), D(0), D(0)
    unknown = []
    for t in trades:
        price, size, commission = D(t['price']), D(t['qty']), D(t['commission'])
        gross += price * size
        qty += size
        asset = t['commissionAsset']
        if asset == 'USDT':
            fee += commission
        elif asset == base:
            fee += commission * price
            base_fee += commission
        elif commission:
            unknown.append({'asset': asset, 'amount': str(commission)})
    return {'gross': gross, 'fee_quote': fee, 'qty': qty, 'base_fee': base_fee, 'unvalued_fees': unknown}
