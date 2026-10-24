import copy
import threading
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from aegis.config import DATA, PORT, SYMBOLS, STRATEGY, validate_settings
from aegis.domain import D, Blocked, floor_step, fingerprint
from aegis.storage import Store
from aegis.vault import Vault
from aegis.binance import Binance, value_assets
from aegis.chatgpt import ChatGPT
from aegis.execution import Broker, fill_cost, FINAL
from aegis.market import BinanceFeed, parse_rules, parse_candles
from aegis.strategy import evaluate, position_event
from aegis.assessment import SCHEMA, validate
from aegis.risk import plan, filters, fresh, rollover
from aegis.transport import RemoteError
from aegis.connectivity import ConnectionGuard
from aegis.qualification import status as qualification_status, require_live_release
from aegis.research import run_screening
from aegis.intelligence.protection import protection_state

class Engine:
    def __init__(self, data_dir=DATA, store=None, api=None, chat=None):
        self.store = store or Store(data_dir / 'live.sqlite3')
        self.vault = Vault(data_dir / 'credentials.dpapi')
        self.api = api or Binance(self.vault)
        self.chat = chat or ChatGPT(self.vault, PORT)
        self.broker = Broker(self.api, self.store)
        self.feed = BinanceFeed(self.api.public) if self.api.status()['configured'] else BinanceFeed()
        self.markets, self.market_errors, self.fees = {}, {}, {}
        self.account, self.account_error, self.value = None, None, None
        self.last_sync, self.last_history, self.last_heartbeat = 0, 0, None
        self.inflight = None
        self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix='assessment')
        self.shutdown = threading.Event()
        self.last_messages = {}
        self.connection = ConnectionGuard()
        self.report_path = Path(data_dir) / 'research' / 'screening.json'
        self.research_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix='historical-screening')
        self.research_future = None
        self.research_state = {'running': False, 'message': 'Historical screening has not run', 'error': None}

    @property
    def state(self): return self.store.state
    @property
    def settings(self): return self.store.settings

    def change(self, fn, event=None):
        with self.store.lock:
            s = copy.deepcopy(self.state)
            fn(s)
            if self.state.get('position') and s.get('position') and self.state['position'].get('exit_requested'):
                s['position']['exit_requested'] = self.state['position']['exit_requested']
            self.store.save(s, event)

    def notice(self, kind, message):
        old = self.last_messages.get(kind)
        if not old or old[0] != message or time.time() - old[1] >= 60:
            self.store.event(kind, message)
            self.last_messages[kind] = (message, time.time())

    def pause(self, reason='New entries paused. Exchange protection remains active.'):
        def change(s):
            if s['mode'] != 'EMERGENCY': s['mode'] = 'PAUSED'
            s['generation'] += 1
        self.change(change, ('control', {'message': reason}))

    def save_settings(self, values):
        values = validate_settings(values)
        if self.state['mode'] != 'PAUSED' or self.state['position'] or self.state.get('pending_entry') or self.broker.unresolved():
            raise Blocked('Pause and resolve the managed position/orders before changing risk settings')
        if values['model'] and values['model'] not in [m['slug'] for m in self.chat.models()]:
            raise Blocked('Select a model returned by your ChatGPT account')
        with self.store.lock:
            s = copy.deepcopy(self.state)
            s['generation'] += 1
            s['peak_bot_equity'] = str(values['allocation']) if values['allocation'] else None
            self.store.save(s, ('settings', {'message': 'Trading settings updated'}), settings=values)

    def clear_model(self):
        with self.store.lock:
            settings = {**self.settings, 'model': None}
            s = copy.deepcopy(self.state)
            s['generation'] += 1
            self.store.save(s, settings=settings)

    def connect_binance(self, values):
        if self.state['mode'] != 'PAUSED' or self.state['position'] or self.state.get('pending_entry') or self.broker.unresolved():
            raise Blocked('Pause and resolve managed orders before changing Binance credentials')
        old = self.vault.read().get('binance')
        account = self.api.connect(values.get('key'), values.get('secret'), values.get('environment', 'live'))
        identity = str(account.get('uid') or fingerprint(values['key']))
        if self.state.get('account_id') and (self.state['account_id'] != identity or self.state.get('account_environment', values.get('environment', 'live')) != values.get('environment', 'live')):
            self.vault.update('binance', old)
            raise Blocked('This ledger belongs to a different Binance account. Use a separate installation for another account.')
        self.change(lambda s: s.update(account_id=identity, account_environment=values.get('environment', 'live'), generation=s['generation'] + 1),
                    ('connection', {'message': 'Binance Spot account verified'}))
        self.account = {**account, 'synced_at': time.time(), 'open_orders': []}
        self.last_sync = 0
        self.markets, self.fees, self.value = {}, {}, None
        self.feed = BinanceFeed(self.api.public)
        return {'ok': True}

    def disconnect_binance(self):
        if self.state['position'] or self.state.get('pending_entry') or self.broker.unresolved():
            raise Blocked('Resolve the managed position and unknown requests before disconnecting Binance')
        self.pause()
        self.vault.update('binance', None)
        self.account, self.value, self.account_error = None, None, None
        self.feed = BinanceFeed()
        self.markets, self.fees = {}, {}
        self.store.event('connection', 'Binance credentials removed from this device')

    def sync_account(self):
        account = self.api.request('account', omitZeroBalances='true')
        identity = str(account.get('uid') or fingerprint(self.api.credentials()['key']))
        if self.state.get('account_id') and identity != self.state['account_id']:
            raise Blocked('Connected Binance identity differs from the trading ledger')
        orders = self.api.request('openOrders')
        tickers = self.api.public('ticker/bookTicker')
        self.value = value_assets(account, tickers)
        self.store.wallet_mark(self.value['value_usdt'], not self.value['unpriced'])
        self.account = {**account, 'synced_at': time.time(), 'open_orders': orders}
        self.account_error = None
        self.last_sync = time.time()
        for symbol in self.settings['symbols']:
            if time.time() - self.fees.get(symbol, {}).get('at', 0) > 240:
                try: self.fees[symbol] = self.api.commission(symbol)
                except Exception as exc: self.notice('commission', str(exc))
        if time.time() - self.last_history > 30:
            for symbol in self.settings['symbols']:
                try:
                    trades = self.api.request('myTrades', symbol=symbol, limit=1000)
                    self.store.merge_trades(symbol, trades)
                except Exception as exc:
                    self.notice('history', str(exc))
            self.last_history = time.time()

    def history(self, symbol, before=None):
        if symbol not in SYMBOLS:
            raise Blocked('Unsupported history symbol')
        params = {'symbol': symbol, 'limit': 1000}
        if before:
            # Binance provides forward pagination; use an endTime from the oldest displayed trade for older records.
            params['endTime'] = int(before) - 1
        rows = self.api.request('myTrades', **params)
        self.store.merge_trades(symbol, rows)
        return {'trades': sorted(rows, key=lambda r: r['time'], reverse=True), 'has_more': len(rows) == 1000}

    def arm(self, confirmation):
        env = self.api.status()['environment']
        require_live_release(env, self.report_path)
        if self.connection.status != 'ONLINE':
            raise Blocked('Wait for fresh market/account data and connection recovery before arming')
        expected = 'ARM LIVE' if env == 'live' else 'ARM TESTNET'
        if confirmation != expected:
            raise Blocked('Enter ' + expected + ' to enable automated orders')
        generation = self.state['generation']
        if self.state['mode'] == 'EMERGENCY' or self.state['position'] or self.state.get('pending_entry') or self.broker.unresolved():
            raise Blocked('Resolve emergency state, managed position and unknown orders first')
        if not self.settings['allocation'] or not self.settings['model']:
            raise Blocked('Set the real allocation and select your ChatGPT model first')
        if self.settings['model'] not in [m['slug'] for m in self.chat.models(refresh=True)]:
            raise Blocked('Selected ChatGPT model is unavailable')
        self.sync_account()
        if not self.account.get('canTrade') or self.account['open_orders']:
            raise Blocked('Trading is unavailable or existing account orders need resolving')
        free = next((D(b['free']) for b in self.account['balances'] if b['asset'] == 'USDT'), D(0))
        if D(self.settings['allocation']) > free:
            raise Blocked(f'Allocation is {D(self.settings["allocation"]):.2f} USDT but only {free:.2f} USDT is free. Entries remain blocked; no balance is invented.')
        symbol = self.settings['symbols'][0]
        book = self.markets.get(symbol)
        fresh(book, time.time())
        qty = floor_step(D(self.settings['allocation']) * D(self.settings['max_position_pct']) / 100 / D(book['ask']), book['rules']['step'])
        filters(qty, book)
        # This validates exchange API trading permission; it does not place an order.
        self.api.request('order/test', 'POST', symbol=symbol, side='BUY', type='MARKET', quantity=str(qty))
        with self.store.lock:
            if self.state['generation'] != generation:
                raise Blocked('Arm request cancelled by a concurrent control change')
            s = copy.deepcopy(self.state)
            s['mode'], s['generation'] = 'RUNNING', generation + 1
            self.store.save(s, ('control', {'message': 'Automated ' + env + ' Spot trading armed for allocation ' + str(self.settings['allocation']) + ' USDT'}))

    def emergency(self):
        def change(s):
            s['mode'] = 'EMERGENCY'
            s['generation'] += 1
            if s['position']: s['position']['exit_requested'] = 'Emergency stop'
        self.change(change, ('control', {'message': 'Emergency stop latched. Only bot-owned orders/position will be cancelled or closed.'}))

    def reset_emergency(self):
        if self.state['position'] or self.state.get('pending_entry') or self.broker.unresolved():
            raise Blocked('Reset requires no managed position or unknown order')
        self.change(lambda s: s.update(mode='PAUSED', generation=s['generation'] + 1), ('control', {'message': 'Emergency reset; entries remain paused'}))

    def _trade_rows(self, symbol, response, buyer):
        oid = response.get('orderId')
        if oid is None: return []
        rows = self.api.request('myTrades', symbol=symbol, orderId=oid, limit=1000)
        if not rows and response.get('fills') and all('tradeId' in r for r in response['fills']):
            rows = [{**r, 'id': r['tradeId'], 'orderId': oid, 'time': response.get('transactTime', int(time.time() * 1000)),
                     'isBuyer': buyer, 'symbol': symbol} for i, r in enumerate(response['fills'])]
        self.store.merge_trades(symbol, rows)
        return rows

    def _resolve_entry(self):
        pending = self.state.get('pending_entry')
        if not pending: return
        intent = self.store.intent(pending['key'])
        if not intent:
            if self.state['mode'] != 'RUNNING':
                self.change(lambda s: s.update(pending_entry=None))
            return
        if intent['status'] in ('SENDING', 'UNKNOWN'):
            self.notice('unresolved', 'Entry status unknown. No second entry will be submitted.')
            return
        response = intent.get('response') or {}
        if intent['status'] == 'REJECTED' or response.get('status') in FINAL and D(response.get('executedQty', 0)) == 0:
            self.change(lambda s: s.update(pending_entry=None), ('rejected', {'message': intent.get('error', 'Entry did not execute')}))
            return
        if response.get('status') not in FINAL:
            if time.time() - intent['created_at'] > 3:
                self.broker.cancel_entry(intent)
                self.notice('entry_pending', 'Entry awaiting terminal exchange status; new orders blocked')
            return
        rows = self._trade_rows(pending['candidate']['symbol'], response, True)
        if not rows or sum(D(t['qty']) for t in rows) != D(response['executedQty']):
            self.notice('reconciliation', 'Entry fill details are incomplete; new orders blocked')
            return
        c = fill_cost(rows, pending['candidate']['symbol'])
        quote_fee = sum(D(t['commission']) for t in rows if t['commissionAsset'] == 'USDT')
        net_qty = c['qty'] - c['base_fee']
        book = self.markets.get(pending['candidate']['symbol'])
        if not book: return
        tradable = floor_step(net_qty, book['rules']['step'])
        if tradable <= 0: raise Blocked('Executed entry is below one tradable increment; manual review required')
        cost = (c['gross'] + quote_fee) * tradable / net_qty
        candidate = pending['candidate']
        position = {'entry_intent': pending['key'], 'symbol': candidate['symbol'], 'qty': str(tradable), 'remaining_qty': str(tradable),
                    'entry_order_id': response['orderId'], 'entry_price': str(c['gross'] / c['qty']), 'cost': str(cost),
                    'stop_loss': candidate['stop_loss'], 'take_profit': candidate['take_profit'], 'opened_at': time.time(),
                    'status': 'NEEDS_PROTECTION', 'protection_revision': 0, 'protection_intent': None,
                    'protection_order_list_id': None, 'sell_order_ids': [], 'assessed_events': [],
                    'unvalued_entry_fees': c['unvalued_fees'], 'exit_requested': None,
                    'unmanaged_base_floor': pending.get('base_before', '0')}
        def change(s):
            s['position'], s['pending_entry'] = position, None
            s['daily_entries'] += 1
            if net_qty > tradable:
                s.setdefault('residuals', []).append({'symbol': candidate['symbol'], 'qty': str(net_qty - tradable), 'cost': str((c['gross'] + quote_fee) * (net_qty-tradable)/net_qty), 'mark_price': str(c['gross']/c['qty']), 'at': time.time(), 'reason': 'Quantity increment after entry commission'})
        self.change(change, ('entry_filled', {'message': 'Real Binance fill confirmed; attaching exchange protection', 'symbol': candidate['symbol'], 'quantity': str(tradable)}))
        self.last_sync = 0

    def _refresh_position(self):
        p = copy.deepcopy(self.state['position'])
        if not p: return
        symbolsells = list(p.get('sell_order_ids', []))
        protection = self.store.intent(p['protection_intent']) if p.get('protection_intent') else None
        active_list = False
        verified_children = []
        oco = None
        if protection and protection['status'] == 'ACCEPTED':
            list_id = (protection.get('response') or {}).get('orderListId')
            if list_id is None: raise Blocked('Protective list ID is unresolved')
            p['protection_order_list_id'] = list_id
            oco = self.api.request('orderList', orderListId=list_id)
            active_list = oco.get('listOrderStatus') != 'ALL_DONE'
            for child in oco.get('orders', []):
                if child['orderId'] not in symbolsells: symbolsells.append(child['orderId'])
                order = self.api.request('order', symbol=p['symbol'], orderId=child['orderId'])
                verified_children.append(order)
                if order.get('status') == 'PARTIALLY_FILLED':
                    p['exit_requested'] = 'Protective order partially filled; close remainder'
        for intent in self.store.intents():
            if intent['kind'] == 'EXIT' and intent['id'].startswith('exit:' + p['entry_intent'] + ':') and (intent.get('response') or {}).get('orderId'):
                oid = intent['response']['orderId']
                if oid not in symbolsells: symbolsells.append(oid)
        rows = []
        for oid in symbolsells:
            order = self.api.request('order', symbol=p['symbol'], orderId=oid)
            ts = self.api.request('myTrades', symbol=p['symbol'], orderId=oid, limit=1000)
            if sum(D(t['qty']) for t in ts) != D(order.get('executedQty', 0)):
                raise Blocked('Sell fill history is incomplete; further sells are blocked until reconciliation')
            self.store.merge_trades(p['symbol'], ts)
            rows.extend(ts)
        unique = list({t['id']: t for t in rows}.values())
        sums = fill_cost(unique, p['symbol'])
        remaining = max(D(0), D(p['qty']) - sums['qty'] - sums['base_fee'])
        p['remaining_qty'], p['sell_order_ids'] = str(remaining), symbolsells
        quote_fees = sum(D(t['commission']) for t in unique if t['commissionAsset'] == 'USDT')
        p['exit_proceeds'] = str(sums['gross'] - quote_fees)
        p['protection_active'] = active_list  # Conservative internal may-be-active cancellation guard.
        p['protection_verified_state'] = protection_state(planned=True,intent=protection,order_list=oco,children=verified_children)
        if remaining == 0:
            quote_fees = sum(D(t['commission']) for t in unique if t['commissionAsset'] == 'USDT')
            gross_pnl = sums['gross'] - quote_fees - D(p['cost'])
            missing = p['unvalued_entry_fees'] + sums['unvalued_fees']
            fee = self.fees.get(p['symbol'], {'buy': '.01', 'sell': '.01'})
            extra_bound = (D(p['cost']) * D(fee['buy']) + sums['gross'] * D(fee['sell'])) if missing else D(0)
            risk_pnl = gross_pnl - extra_bound
            closed = {**p, 'closed_at': time.time(), 'net_pnl': str(gross_pnl) if not missing else None,
                      'quote_pnl_before_unvalued_fees': str(gross_pnl), 'unvalued_fees': missing, 'risk_pnl': str(risk_pnl)}
            with self.store.lock, self.store.db:
                s = copy.deepcopy(self.state)
                s.update(position=None, realized=str(D(s['realized']) + risk_pnl),
                         daily_realized=str(D(s['daily_realized']) + risk_pnl), last_exit_at=time.time(),
                         consecutive_losses=s['consecutive_losses'] + 1 if risk_pnl < 0 else 0)
                s['pnl_unvalued'] = s.get('pnl_unvalued', False) or bool(missing)
                self.store.record_session(closed) if hasattr(self.store,'record_session') else self.store.db.execute('INSERT INTO sessions(at,body) VALUES(?,?)', (time.time(), __import__('json').dumps(closed)))
                self.store.save(s, ('position_closed', {'message': 'Managed position closed on Binance', 'trade': closed}))
            self.last_sync = 0
        else:
            self.change(lambda s: s.update(position=p))

    def _manage_position(self):
        if not self.state['position']: return
        self._refresh_position()
        p = copy.deepcopy(self.state['position'])
        if not p: return
        book = self.markets.get(p['symbol'])
        if self.state['mode'] == 'EMERGENCY': p['exit_requested'] = 'Emergency stop'
        if self.state['daily_halted']: p['exit_requested'] = 'Daily loss limit'
        if p.get('exit_requested'):
            if self.broker.unresolved():
                self.notice('unresolved', 'Exit waits for an ambiguous exchange request to reconcile')
                return
            if p.get('protection_active'):
                result = self.broker.cancel_protection(p)
                if not result or result['status'] != 'ACCEPTED':
                    self.notice('exit', 'Waiting for native protective order cancellation')
                    return
                self._refresh_position()
                if not self.state['position']: return
                p = copy.deepcopy(self.state['position'])
                if p.get('protection_active'):
                    self.notice('exit', 'Protective orders are still active; a second sell is blocked')
                    return
            self.sync_account()
            self._refresh_position()
            if not self.state['position']: return
            p = copy.deepcopy(self.state['position'])
            try:
                open_exits = [i for i in self.store.intents() if i['kind'] == 'EXIT' and i['id'].startswith('exit:' + p['entry_intent'] + ':') and i['status'] != 'REJECTED' and (i.get('response') or {}).get('status') not in FINAL]
                if open_exits: return
                result = self.broker.exit(p, book, self.account)
                p['status'] = 'EXIT_PENDING' if result['status'] != 'REJECTED' else 'EXIT_BLOCKED'
                self.change(lambda s: s.update(position=p))
            except Blocked as exc:
                self.notice('exit_blocked', str(exc))
            return
        if p.get('protection_intent'):
            intent = self.store.intent(p['protection_intent'])
            if intent and intent['status'] in ('SENDING', 'UNKNOWN'):
                self.notice('protection', 'Protective order status is being reconciled')
                return
            if intent and intent['status'] == 'ACCEPTED' and p.get('protection_active'):
                p['status'] = 'PROTECTED'
                self.change(lambda s: s.update(position=p))
                return
            if intent and intent['status'] == 'REJECTED':
                p['exit_requested'] = 'Exchange protection rejected; close exposure'
                self.change(lambda s: s.update(position=p))
                return
            p['protection_revision'] += 1
            p['protection_intent'], p['protection_order_list_id'] = None, None
        try:
            managed_account = self.api.request('account', omitZeroBalances='true')
            result = self.broker.protection(p, book, managed_account)
            p['protection_intent'] = result['id']
            if result['status'] == 'ACCEPTED':
                p['protection_order_list_id'] = result['response']['orderListId']
                p['status'] = 'PROTECTED'
            elif result['status'] == 'REJECTED':
                p['status'] = 'PROTECTION_REJECTED'
                p['exit_requested'] = 'Native protection rejected; close exposure'
            else: p['status'] = 'PROTECTION_PENDING'
            self.change(lambda s: s.update(position=p))
        except Blocked as exc:
            p['exit_requested'] = str(exc)
            self.change(lambda s: s.update(position=p))
            self.notice('protection', str(exc))

    def _risk_state(self):
        def update(s):
            rollover(s, time.time())
            allocation = D(self.settings['allocation'] or 0)
            eq = allocation + D(s['realized'])
            p = s['position']
            if p and p['symbol'] in self.markets:
                eq += D(p['remaining_qty']) * D(self.markets[p['symbol']]['bid']) + D(p.get('exit_proceeds', 0)) - D(p['cost'])
            peak = max(D(s.get('peak_bot_equity') or allocation), eq)
            s['peak_bot_equity'] = str(peak)
            if p or s['daily_entries'] or D(s['realized']):
                s['max_bot_drawdown'] = str(max(D(s.get('max_bot_drawdown', 0)), peak - eq))
            if -D(s['daily_realized']) >= D(self.settings['daily_loss_limit']): s['daily_halted'] = True
            if peak > 0 and eq <= peak * (1 - D(self.settings['max_drawdown_pct']) / 100): s['daily_halted'] = True
            if p and -D(s['daily_realized']) + max(D(0), D(p['cost']) - D(p.get('exit_proceeds', 0)) - D(p['remaining_qty']) * D(self.markets.get(p['symbol'], {}).get('bid', p['entry_price']))) >= D(self.settings['daily_loss_limit']):
                s['daily_halted'] = True
        self.change(update)

    def _schedule(self):
        # Candidate build: all runtime inference goes through the isolated economy gate.
        # Legacy order management remains model-independent. No paid activation here.
        return

    def _receive(self):
        if not self.inflight or not self.inflight['future'].done(): return
        job, self.inflight = self.inflight, None
        try:
            p, usage = job['future'].result()
            self.change(lambda s: s['usage'].update(calls=s['usage']['calls'] + 1,
                        input_tokens=s['usage']['input_tokens'] + int(usage.get('input_tokens', 0)),
                        output_tokens=s['usage']['output_tokens'] + int(usage.get('output_tokens', 0))))
            validate(p, job['context'], time.time())
            self.store.event('decision', p['explanation'], proposal=p)
            if self.state['generation'] != job['generation'] or self.state['mode'] != 'RUNNING':
                raise Blocked('Assessment invalidated by control change')
            if p['decision'] == 'BUY':
                require_live_release(self.api.status()['environment'], self.report_path)
                if self.connection.status != 'ONLINE': raise Blocked('Connection is not healthy; entry cancelled')
                context = job['context']
                sized = plan(self.state, self.settings, self.account, context, self.markets.get(context['symbol']),
                             self.fees.get(context['symbol']), time.time(), bool(self.broker.unresolved()))
                key = 'entry:' + context['id']
                if self.store.intent(key): raise Blocked('Duplicate candidate order blocked')
                base_before = next((str(D(b['free']) + D(b['locked'])) for b in self.account['balances'] if b['asset'] == context['symbol'][:-4]), '0')
                self.change(lambda s: s.update(pending_entry={'candidate': context, 'plan': sized, 'key': key, 'base_before': base_before}))
                self.broker.entry(context, sized, job['generation'])
                # Attach protection immediately after a known fill, before routine account/history polling.
                self._resolve_entry()
                self._manage_position()
            elif p['decision'] == 'EXIT':
                if not self.state['position'] or self.state['position']['entry_intent'] != job['context'].get('position_id'):
                    raise Blocked('Managed position changed while model was running')
                self.change(lambda s: s['position'].update(exit_requested='ChatGPT EXIT: ' + p['explanation']))
        except Exception as exc:
            self.notice('wait', str(exc) if isinstance(exc, (Blocked, RemoteError)) else 'Assessment unavailable; WAIT')

    def cycle(self):
        self._risk_state()
        self.broker.reconcile()
        self._resolve_entry()
        self._manage_position()
        self._receive()
        self._schedule()
        self.last_heartbeat = time.time()

    def start_research(self):
        if self.research_future and not self.research_future.done():
            raise Blocked('Historical screening is already running')
        if self.state['mode'] != 'PAUSED' or self.state['position'] or self.state.get('pending_entry') or self.broker.unresolved():
            raise Blocked('Historical screening requires paused entries and resolved managed exposure')
        self.research_state = {'running': True, 'message': 'Preparing a read-only 90-day historical screen', 'error': None}
        settings = copy.deepcopy(self.settings)
        def progress(message): self.research_state['message'] = message
        from aegis.market import public_get
        def getter(route, **params):
            if self.shutdown.is_set(): raise Blocked('Historical screening stopped with the application')
            return public_get(route, **params)
        self.research_future = self.research_pool.submit(run_screening, settings, self.report_path, 90, progress, getter)
        self.store.event('research', 'Read-only historical screening started. No connected-wallet orders are permitted.')
        return {'ok': True}

    def poll_research(self):
        if not self.research_future or not self.research_future.done(): return
        future, self.research_future = self.research_future, None
        try:
            future.result()
            self.research_state = {'running': False, 'message': 'Historical screening completed; live release remains locked', 'error': None}
            self.store.event('research', 'Historical screening completed. Results are evidence, not a profit guarantee or live release.')
        except Exception as exc:
            message = str(exc) if isinstance(exc, (Blocked, RemoteError)) else 'Historical data unavailable or screening failed; no live release'
            self.research_state = {'running': False, 'message': 'Screening did not complete', 'error': message}
            self.store.event('research', message)

    def run(self):
        while not self.shutdown.is_set():
            try:
                feed = self.feed
                markets, errors = feed.fetch_all(SYMBOLS)
                if feed is not self.feed: continue
                self.markets.update(markets); self.market_errors = errors
                configured = self.api.status()['configured']
                if configured and time.time() - self.last_sync > 10:
                    try: self.sync_account()
                    except Exception as exc:
                        self.account_error = str(exc) if isinstance(exc, (Blocked, RemoteError)) else 'Account refresh failed'
                        self.notice('account', self.account_error)
                now = time.time()
                quotes_ok = all(s in self.markets and 0 <= now-self.markets[s]['received_at'] <= 10 and s not in errors for s in self.settings['symbols'])
                account_ok = not configured or bool(self.account and now-self.account['synced_at'] <= 20 and not self.account_error)
                if self.connection.observe(quotes_ok and account_ok, now, 'Fresh market/account data is unavailable; new entries paused'):
                    self.pause('Connection/data failure: new entries paused, proposals invalidated. Existing native exchange protection remains active.')
                if configured: self.cycle()
                else: self.last_heartbeat = now
                self.poll_research()
            except Exception as exc:
                if self.connection.observe(False, time.time(), 'Worker or exchange communication failed; awaiting recovery'):
                    self.pause('Worker/connection failure paused new entries. Existing native exchange orders remain on Binance.')
                self.notice('worker', str(exc) if isinstance(exc, (Blocked, RemoteError)) else 'Worker cycle failed; entries remain paused until recovery')
                self.poll_research()
            delay = max(3, (self.connection.next_retry_at or time.time())-time.time())
            self.shutdown.wait(min(delay,30))

    def performance(self):
        with self.store.lock:
            import json
            rows = [json.loads(r[0]) for r in self.store.db.execute('SELECT body FROM sessions ORDER BY id')]
        known = [D(r['net_pnl']) for r in rows if r.get('net_pnl') is not None]
        wins, losses = [v for v in known if v > 0], [v for v in known if v < 0]
        total, peak, drawdown, curve = D(0), D(0), D(0), []
        for r in rows:
            if r.get('net_pnl') is None: continue
            total += D(r['net_pnl']); peak = max(peak, total); drawdown = max(drawdown, peak-total)
            curve.append({'at': r['closed_at'], 'pnl': str(total)})
        return {'closed_trades': len(rows), 'unvalued': len(rows)-len(known), 'net_pnl': str(total) if known and len(known)==len(rows) else None,
                'win_rate': float(len(wins)/len(known)*100) if known else None,
                'average_win': str(sum(wins)/len(wins)) if wins else None,
                'average_loss': str(sum(losses)/len(losses)) if losses else None,
                'profit_factor': str(sum(wins)/-sum(losses)) if losses else None,
                'max_drawdown_usdt': str(drawdown) if known else None, 'curve': curve}

    def snapshot(self):
        with self.store.lock:
            s = copy.deepcopy(self.state)
            intents = self.store.intents()
            account = None
            if self.account:
                account = {'can_trade': self.account.get('canTrade', False), 'synced_at': self.account['synced_at'],
                           'stale': time.time() - self.account['synced_at'] > 20,
                           'open_orders': self.account['open_orders'], **(self.value or {'assets': [], 'value_usdt': None, 'unpriced': []})}
            return {'state': s, 'settings': self.settings, 'binance': {**self.api.status(), 'error': self.account_error},
                    'chatgpt': self.chat.status(), 'account': account,
                    'markets': [{k: v for k, v in m.items() if k not in ('bids', 'asks', 'candles')} for m in self.markets.values()],
                    'events': self.store.events(), 'performance': self.performance(), 'wallet_curve': self.store.wallet_curve(), 'trades': self.store.trades(), 'intents': intents[:100],
                    'unresolved': len(self.broker.unresolved()), 'assessing': bool(self.inflight),
                    'heartbeat': self.last_heartbeat, 'server_time': time.time(), 'market_errors': self.market_errors,
                    'connectivity': self.connection.snapshot(), 'qualification': qualification_status(self.report_path),
                    'research': copy.deepcopy(self.research_state)}

    def close(self):
        self.shutdown.set()
        self.pause('Application stopping; new entries paused. Native exchange orders remain on Binance.')
        self.pool.shutdown(wait=False, cancel_futures=True)
        self.research_pool.shutdown(wait=False, cancel_futures=True)
