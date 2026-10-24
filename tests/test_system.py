import copy
import hashlib
import hmac
import io
import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from unittest.mock import patch, Mock
from aegis.domain import D, Blocked
from aegis.storage import Store
from aegis.vault import Vault
from aegis.binance import Binance, value_assets
from aegis.chatgpt import ChatGPT, SCOPES
from aegis.execution import Broker, fill_cost
from aegis.engine import Engine
from aegis.config import DEFAULTS, validate_settings, STRATEGY
from aegis.risk import plan, rollover
from aegis.assessment import validate, SCHEMA
from aegis.market import parse_candles, parse_rules
from aegis.strategy import evaluate
from aegis.transport import RemoteError
from aegis.process_lock import process_lock

# Explicit test fixtures. These values are never imported by the application.
def book():
    return {'symbol':'BTCUSDT','received_at':time.time(),'bid':100,'ask':100,'mid':100,'spread_bps':0,
            'rules':{'enabled':True,'step':'.001','tick':'.01','min_qty':'.001','max_qty':'100',
                     'min_notional':'5','max_notional':None,'avg_windows':[0]},'avg_mins':0,'avg_price':100}
def candidate():
    return {'id':'candidate-one','symbol':'BTCUSDT','entry':'100','stop_loss':'99','take_profit':'103',
            'created_at':time.time(),'expires_at':time.time()+80,'event':'candidate','tick':'.01'}
def account():
    return {'uid':12,'accountType':'SPOT','canTrade':True,'synced_at':time.time(),'open_orders':[],
            'balances':[{'asset':'USDT','free':'100','locked':'0'},{'asset':'BTC','free':'.1','locked':'0'}]}
def settings(): return {**DEFAULTS,'symbols':['BTCUSDT'],'allocation':10,'risk_per_trade':.2,'model':'account-returned-model'}
def fee(): return {'buy':'.001','sell':'.001','at':time.time()}
def trade(qty='.1',price='100',commission='0.01',asset='USDT',id=1,order=7,buyer=True):
    return {'id':id,'orderId':order,'qty':qty,'price':price,'commission':commission,
            'commissionAsset':asset,'isBuyer':buyer,'time':int(time.time()*1000),'quoteQty':str(D(qty)*D(price))}

class FakeAPI:
    def __init__(self): self.calls=[]; self.rows={}; self.orders={}; self.lists={}; self.handler=None
    def status(self): return {'configured':True,'environment':'testnet'}
    def request(self,route,method='GET',**p):
        self.calls.append((route,method,p))
        if self.handler:
            v=self.handler(route,method,p)
            if v is not None: return v
        if route=='myTrades': return self.rows.get(p.get('orderId'),[])
        if route=='orderList': return self.lists.get(p.get('orderListId'),{'orderListId':9,'listOrderStatus':'EXECUTING','orders':[]})
        if route=='order' and method=='GET': return self.orders.get(p.get('orderId'),{'orderId':p.get('orderId',7),'status':'FILLED','executedQty':str(sum(D(t['qty']) for t in self.rows.get(p.get('orderId'),[]))) if p.get('orderId') is not None else '.1'})
        if route=='account': return account()
        if route=='openOrders': return []
        if route=='orderList/oco': return {'orderListId':9,'listOrderStatus':'EXECUTING','orders':[]}
        return {'orderId':7,'status':'FILLED','executedQty':'.1'}
    def credentials(self): return {'key':'test-key'}
    def public(self,route,**p): return []
    def commission(self,symbol): return fee()

class Base(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.path=Path(self.temp.name);self.addCleanup(self.temp.cleanup)
        self.store=Store(self.path/'ledger.sqlite3');self.addCleanup(self.store.db.close); self.api=FakeAPI(); self.broker=Broker(self.api,self.store)
        self.store.state['mode']='RUNNING'; self.store.state['daily_date']='2099-01-01'
    def engine(self):
        e=Engine(self.path,store=self.store,api=self.api,chat=Mock())
        e.settings.update(settings());e.markets={'BTCUSDT':book()};e.account=account();e.fees={'BTCUSDT':fee()}
        self.addCleanup(lambda:e.pool.shutdown(wait=False,cancel_futures=True));return e
    def pending(self):
        c=candidate();key='entry:'+c['id']
        self.store.state['pending_entry']={'key':key,'candidate':c,'plan':{'qty':'.1'}}
        self.broker.entry(c,{'qty':'.1'},self.store.state['generation']);return key
    def position(self):
        return {'entry_intent':'entry:one','symbol':'BTCUSDT','qty':'.1','remaining_qty':'.1','cost':'10.01',
                'entry_price':'100','entry_order_id':7,'stop_loss':'99','take_profit':'103','opened_at':time.time(),
                'status':'NEEDS_PROTECTION','protection_revision':0,'protection_intent':None,'protection_order_list_id':None,
                'sell_order_ids':[],'assessed_events':[],'unvalued_entry_fees':[],'exit_requested':None}

class RiskTests(Base):
    def test_no_real_allocation_no_order(self):
        with self.assertRaises(Blocked):plan(self.store.state,DEFAULTS,account(),candidate(),book(),fee(),time.time(),False)
    def test_sized_from_real_funds_and_fee_risk(self):
        p=plan(self.store.state,settings(),account(),candidate(),book(),fee(),time.time(),False)
        self.assertLessEqual(D(p['qty'])*D(100),D(8));self.assertLessEqual(D(p['risk']),D('.2'))
    def test_minimum_does_not_inflate_small_allocation(self):
        with self.assertRaises(Blocked):plan(self.store.state,{**settings(),'allocation':5},account(),candidate(),book(),fee(),time.time(),False)
    def test_entry_commission_must_leave_sellable_notional(self):
        b=book();b['rules']['min_notional']='7.8'
        with self.assertRaises(Blocked):plan(self.store.state,settings(),account(),candidate(),b,fee(),time.time(),False)
    def test_stale_account_and_market_block(self):
        for field in ('account','market'):
            with self.subTest(field=field):
                a,b=account(),book()
                if field=='account':a['synced_at']-=30
                else:b['received_at']-=30
                with self.assertRaises(Blocked):plan(self.store.state,settings(),a,candidate(),b,fee(),time.time(),False)
    def test_loss_position_order_and_spread_blocks(self):
        variants=[({'position':{}},None),({'daily_halted':True},None),({'consecutive_losses':3},None),({'daily_entries':5},None),({},'orders'),({},'spread'),({},'fees'),({},'cash'),({},'unknown')]
        for fields,case in variants:
            with self.subTest(case=case,fields=fields):
                s={**self.store.state,**fields};a=account();b=book();f=fee()
                if 'position' in fields:s['position']={'symbol':'BTCUSDT'}
                if case=='orders':a['open_orders']=[{'orderId':123}]
                if case=='spread':b['spread_bps']=100
                if case=='fees':f=None
                if case=='cash':a['balances']=[]
                with self.assertRaises(Blocked):plan(s,settings(),a,candidate(),b,f,time.time(),case=='unknown')
    def test_remaining_daily_loss_budget_enforced(self):
        self.store.state['daily_realized']='-.49'
        with self.assertRaises(Blocked):plan(self.store.state,settings(),account(),candidate(),book(),fee(),time.time(),False)
    def test_utc_rollover_does_not_reset_lifetime_pnl(self):
        s={**self.store.state,'realized':'-2','daily_realized':'-1','daily_halted':True};rollover(s,time.time())
        self.assertEqual(s['realized'],'-2');self.assertEqual(s['daily_realized'],'0');self.assertFalse(s['daily_halted'])
    def test_reject_nan_and_unknown_settings(self):
        for v in ({**settings(),'allocation':float('nan')},{**settings(),'leverage':10}):
            with self.assertRaises(Blocked):validate_settings(v)

class ExecutionTests(Base):
    def test_intent_is_persisted_before_network_and_duplicate_is_not_resent(self):
        c=candidate();key='entry:'+c['id']
        def handler(route,method,p):
            self.assertEqual(self.store.intent(key)['status'],'SENDING');return {'orderId':7,'status':'FILLED'}
        self.api.handler=handler
        self.broker.entry(c,{'qty':'.1'},self.store.state['generation']);self.broker.entry(c,{'qty':'.1'},self.store.state['generation'])
        self.assertEqual(len(self.api.calls),1)
    def test_unknown_post_survives_restart_and_is_reconciled_not_resent(self):
        def handler(route,method,p):
            if method=='POST':raise RemoteError('Binance',504,unknown=True)
            return {'orderId':7,'status':'FILLED','executedQty':'.1'}
        self.api.handler=handler;key=self.pending();self.assertEqual(self.store.intent(key)['status'],'UNKNOWN')
        other=Store(self.path/'ledger.sqlite3');self.addCleanup(other.db.close)
        b=Broker(self.api,other);b.reconcile();self.assertEqual(other.intent(key)['response']['orderId'],7)
        self.assertEqual(sum(m=='POST' for _,m,_ in self.api.calls),1);self.assertEqual(other.state['mode'],'PAUSED')
    def test_absent_unknown_order_is_not_retried(self):
        def handler(route,method,p):raise RemoteError('Binance',504,unknown=True) if method=='POST' else RemoteError('Binance',400,-2013)
        self.api.handler=handler;key=self.pending();self.broker.reconcile()
        self.assertEqual(self.store.intent(key)['status'],'UNKNOWN');self.assertEqual(len(self.broker.unresolved()),1)
    def test_pause_generation_prevents_send(self):
        with self.assertRaises(Blocked):self.broker.entry(candidate(),{'qty':'.1'},self.store.state['generation']-1)
        self.assertFalse(self.api.calls)
    def test_native_oco_has_exact_stop_target_and_bot_identity(self):
        r=self.broker.protection(self.position(),book());route,method,p=self.api.calls[-1]
        self.assertEqual(route,'orderList/oco');self.assertEqual(p['belowType'],'STOP_LOSS');self.assertEqual(p['aboveType'],'LIMIT_MAKER')
        self.assertEqual(p['belowStopPrice'],'99');self.assertEqual(p['abovePrice'],'103');self.assertLessEqual(len(p['listClientOrderId']),36)
    def test_cancellation_only_uses_owned_list(self):
        p=self.position();self.assertIsNone(self.broker.cancel_protection(p));self.assertFalse(self.api.calls)
        p['protection_order_list_id']=9;self.broker.cancel_protection(p)
        self.assertEqual(self.api.calls[0],('orderList','DELETE',{'symbol':'BTCUSDT','orderListId':9}))
    def test_exit_cannot_sell_existing_extra_holdings(self):
        a=account();a['balances'][1]['free']='5';self.broker.exit(self.position(),book(),a)
        self.assertEqual(D(self.api.calls[-1][2]['quantity']),D('.1'))
    def test_manual_balance_depletion_cannot_spend_preexisting_holdings(self):
        a=account();a['balances'][1]['free']='5.04';p=self.position();p['unmanaged_base_floor']='5'
        with self.assertRaises(Blocked):self.broker.exit(p,book(),a)
        with self.assertRaises(Blocked):self.broker.protection(p,book(),a)
        self.assertFalse(self.api.calls)
    def test_exact_fill_commissions_preserve_unpriced_asset(self):
        c=fill_cost([trade(asset='BTC',commission='.0001'),trade(id=2,asset='BNB',commission='.001')],'BTCUSDT')
        self.assertEqual(c['base_fee'],D('.0001'));self.assertEqual(c['fee_quote'],D('.01'));self.assertEqual(c['unvalued_fees'],[{'asset':'BNB','amount':'0.001'}])

class EngineTests(Base):
    def test_real_partial_entry_then_cancel_final_attaches_only_filled_quantity(self):
        e=self.engine();key=self.pending();i=self.store.intent(key);i['response'].update(status='PARTIALLY_FILLED',executedQty='.06');i['created_at']-=10
        self.store.save(self.store.state,intent=i);e._resolve_entry()
        self.assertIsNone(e.state['position']);self.assertTrue(any(m=='DELETE' for _,m,_ in self.api.calls))
        i['response'].update(status='CANCELED');self.store.save(self.store.state,intent=i);self.api.rows[7]=[trade(qty='.06',commission='0')]
        e._resolve_entry();self.assertEqual(D(e.state['position']['qty']),D('.06'));e._manage_position()
        self.assertEqual(e.state['position']['protection_order_list_id'],9)
    def test_unknown_protection_recovers_exchange_list_id(self):
        e=self.engine();p=self.position()
        def handler(route,method,params):
            if method=='POST':raise RemoteError('Binance',504,unknown=True)
            if route=='orderList':return {'orderListId':99,'listOrderStatus':'EXECUTING','orders':[]}
        self.api.handler=handler;intent=self.broker.protection(p,book());p['protection_intent']=intent['id'];e.state['position']=p
        self.broker.reconcile();e._refresh_position();self.assertEqual(e.state['position']['protection_order_list_id'],99);self.assertTrue(e.state['position']['protection_active'])
        self.assertEqual(sum(m=='POST' for _,m,_ in self.api.calls),1)
    def test_model_unavailable_does_not_remove_native_stop(self):
        e=self.engine();p=self.position();intent=self.broker.protection(p,book());p.update(protection_intent=intent['id'],protection_order_list_id=9);e.state['position']=p
        e.chat.assess.side_effect=Blocked('model unavailable');e._manage_position()
        self.assertEqual(e.state['position']['status'],'PROTECTED');self.assertFalse(any(m=='DELETE' for _,m,_ in self.api.calls))
    def test_partial_exit_does_not_generate_overlapping_sell(self):
        e=self.engine();p=self.position();p['exit_requested']='User exit';e.state['position']=p
        self.api.handler=lambda r,m,x: {'orderId':8,'status':'PARTIALLY_FILLED','executedQty':'.04'} if r=='order' and m=='POST' else None
        self.broker.exit(p,book(),account());self.api.rows[8]=[trade(qty='.04',price='101',commission='0',id=2,order=8,buyer=False)]
        e._manage_position();self.assertEqual(sum(r=='order' and m=='POST' for r,m,_ in self.api.calls),1)
    def test_lagging_trade_history_cannot_cause_an_extra_sell(self):
        e=self.engine();p=self.position();p.update(sell_order_ids=[8],exit_requested='Exit');e.state['position']=p
        self.api.orders[8]={'orderId':8,'status':'FILLED','executedQty':'.1'}
        self.api.rows[8]=[trade(qty='.04',id=2,order=8,buyer=False)]
        with self.assertRaises(Blocked):e._manage_position()
        self.assertFalse(any(m=='POST' for _,m,_ in self.api.calls))
    def test_partial_proceeds_not_counted_as_drawdown(self):
        e=self.engine();p=self.position();p.update(remaining_qty='.05',exit_proceeds='5.00',cost='10.00');e.state['position']=p
        e._risk_state();self.assertFalse(e.state['daily_halted']);self.assertEqual(D(e.state['peak_bot_equity']),D(10))
    def test_fill_accounting_records_realized_pnl_and_fee(self):
        e=self.engine();p=self.position();p['sell_order_ids']=[8];e.state['position']=p
        self.api.rows[8]=[trade(price='103',commission='.0103',id=2,order=8,buyer=False)];e._refresh_position()
        self.assertIsNone(e.state['position']);self.assertEqual(D(e.performance()['net_pnl']),D('.2797'));self.assertEqual(e.performance()['win_rate'],100)
    def test_non_quote_fee_cannot_be_reported_as_known_net_profit(self):
        e=self.engine();p=self.position();p['sell_order_ids']=[8];e.state['position']=p
        self.api.rows[8]=[trade(price='103',commission='.001',asset='BNB',id=2,order=8,buyer=False)];e._refresh_position()
        self.assertIsNone(e.performance()['net_pnl']);self.assertEqual(e.performance()['unvalued'],1)
    def test_unsubmitted_pending_is_cleared_after_pause(self):
        e=self.engine();e.state.update(pending_entry={'key':'entry:never-sent'},mode='PAUSED');e._resolve_entry();self.assertIsNone(e.state['pending_entry'])
    def test_emergency_survives_restart(self):
        e=self.engine();e.emergency();other=Store(self.path/'ledger.sqlite3');self.addCleanup(other.db.close);self.assertEqual(other.state['mode'],'EMERGENCY')
    def test_chatgpt_account_change_clears_model_without_weakening_exits(self):
        e=self.engine();e.state['position']=self.position();e.pause();e.clear_model()
        self.assertIsNone(e.settings['model']);self.assertEqual(e.state['position']['stop_loss'],'99');self.assertEqual(e.state['mode'],'PAUSED')
    def test_wallet_history_is_only_observed_marks(self):
        self.assertFalse(self.store.wallet_curve());self.store.wallet_mark('12.34',True)
        points=self.store.wallet_curve();self.assertEqual(len(points),1);self.assertEqual(points[0]['value'],'12.34');self.assertTrue(points[0]['complete'])

class IdentityTests(Base):
    def chat(self):return ChatGPT(Vault(self.path/'oauth.dpapi'),8765)
    def profile(self,chat,expires=None,scopes=None):
        chat.vault.update('chatgpt',{'active':'p','profiles':{'p':{'name':'Account','email':'a@example.invalid','client_id':'issued-client','sub':'identity','access_token':'test-access','refresh_token':'test-refresh','scopes':scopes or SCOPES.split(),'expires_at':expires or time.time()+300}}})
    def test_dpapi_roundtrip_no_plaintext(self):
        v=Vault(self.path/'vault');v.update('secret','unit-test-sensitive');self.assertNotIn(b'unit-test-sensitive',v.path.read_bytes());self.assertEqual(v.read()['secret'],'unit-test-sensitive')
    def test_pkce_nonce_state_and_stable_host_id(self):
        c=self.chat();u=parse_qs(urlsplit(c.begin()).query);v=parse_qs(urlsplit(c.begin()).query)
        self.assertEqual(u['client_id'],['dynamic_agent_client']);self.assertEqual(u['ext_agent_host_id'],v['ext_agent_host_id']);self.assertNotEqual(u['nonce'],v['nonce']);self.assertNotEqual(u['state'],v['state']);self.assertEqual(u['code_challenge_method'],['S256'])
        with self.assertRaises(Blocked):c.callback({'state':'forged','code':'code','client_id':'issued'})
    def test_reauth_rejects_registration_change_before_exchange(self):
        c=self.chat();self.profile(c);c.begin('p')
        with patch.object(c,'token_request') as req:
            with self.assertRaises(Blocked):c.callback({'state':c.pending['state'],'client_id':'other','code':'code'})
            req.assert_not_called()
    def test_refresh_is_serialized_and_rotated(self):
        c=self.chat();self.profile(c,time.time()-10)
        def rotate(values):time.sleep(.03);self.assertNotIn('scope',values);return {'access_token':'fresh','refresh_token':'rotated','expires_in':300}
        with patch.object(c,'token_request',side_effect=rotate) as req:
            out=[];threads=[threading.Thread(target=lambda:out.append(c.access_token())) for _ in range(5)]
            [t.start() for t in threads];[t.join() for t in threads]
            self.assertEqual(req.call_count,1);self.assertEqual(out,['fresh']*5);self.assertEqual(c.active()['refresh_token'],'rotated')
    def test_missing_plan_permission_blocks_inference(self):
        c=self.chat();self.profile(c,scopes=['openid']);
        with self.assertRaises(Blocked):c.access_token()
    def test_catalog_only_server_visible_models(self):
        c=self.chat();self.profile(c)
        with patch('aegis.chatgpt.call',return_value={'models':[{'slug':'allowed','display_name':'Returned name','visibility':'list'},{'slug':'hidden','visibility':'hide'}]}):
            self.assertEqual(c.models(),[{'slug':'allowed','name':'Returned name'}])
    def test_stream_completion_and_supported_subscription_payload(self):
        c=self.chat();self.profile(c);c.catalog=[{'slug':'account-returned-model','name':'Available'}];c.catalog_at=time.time()
        response={'decision':'WAIT'};events=[{'type':'response.output_text.delta','delta':json.dumps(response)},{'type':'response.completed','response':{'usage':{'input_tokens':10}}}]
        raw=''.join('data: '+json.dumps(x)+'\n\n' for x in events).encode()
        def open_req(req,timeout):
            body=json.loads(req.data);self.assertFalse(body['store']);self.assertTrue(body['stream']);self.assertIsInstance(body['input'],list);self.assertNotIn('max_output_tokens',body);self.assertEqual(req.get_header('Authorization'),'Bearer test-access');return io.BytesIO(raw)
        with patch('aegis.chatgpt.open_request',side_effect=open_req):
            p,u=c.assess(candidate(),{},'account-returned-model',SCHEMA);self.assertEqual(p,response);self.assertEqual(u['input_tokens'],10)
        with patch('aegis.chatgpt.open_request',return_value=io.BytesIO(b'data: {"type":"response.incomplete"}\n\n')):
            with self.assertRaises(Blocked):c.assess(candidate(),{},'account-returned-model',SCHEMA)
    def test_status_never_exposes_credentials(self):
        c=self.chat();self.profile(c);out=json.dumps(c.status());self.assertNotIn('test-access',out);self.assertNotIn('test-refresh',out)
    def test_oidc_signature_nonce_audience_and_identity_are_verified(self):
        import jwt
        from cryptography.hazmat.primitives.asymmetric import rsa
        key=rsa.generate_private_key(public_exponent=65537,key_size=2048)
        for failure in (None,'nonce','audience','signature','identity'):
            with self.subTest(failure=failure):
                c=self.chat();self.profile(c);c.begin('p');state=c.pending['state'];nonce=c.pending['nonce']
                claims={'iss':'https://auth.openai.com','aud':'wrong' if failure=='audience' else 'issued-client',
                        'sub':'wrong' if failure=='identity' else 'identity','exp':int(time.time()+300),
                        'nonce':'wrong' if failure=='nonce' else nonce}
                signing_key=rsa.generate_private_key(public_exponent=65537,key_size=2048) if failure=='signature' else key
                id_token=jwt.encode(claims,signing_key,algorithm='RS256',headers={'kid':'key'})
                tokens={'id_token':id_token,'access_token':'new','refresh_token':'rotated','scope':SCOPES,'expires_in':300}
                c.discovery={'issuer':'https://auth.openai.com','jwks_uri':'https://auth.openai.com/keys'}
                with patch.object(c,'token_request',return_value=tokens),patch('aegis.chatgpt.jwt.PyJWKClient') as jwks:
                    jwks.return_value.get_signing_key_from_jwt.return_value=Mock(key=key.public_key())
                    if failure:
                        with self.assertRaises((Blocked,jwt.InvalidTokenError)):c.callback({'state':state,'client_id':'issued-client','code':'short-lived-code'})
                    else:
                        self.assertTrue(c.callback({'state':state,'client_id':'issued-client','code':'short-lived-code'})['plan_enabled'])
    def test_issued_client_is_retained_for_retry_after_exchange_failure(self):
        c=self.chat();c.begin();state=c.pending['state']
        with patch.object(c,'token_request',side_effect=RemoteError('ChatGPT',503)):
            with self.assertRaises(Blocked):c.callback({'state':state,'client_id':'registered-id','code':'code'})
        self.assertEqual(parse_qs(urlsplit(c.begin()).query)['client_id'],['registered-id'])

class AdapterTests(Base):
    def test_market_must_support_required_native_exit_types(self):
        p={'status':'TRADING','isSpotTradingAllowed':True,'ocoAllowed':True,'orderTypes':['MARKET','STOP_LOSS','LIMIT_MAKER'],
           'filters':[{'filterType':'LOT_SIZE','minQty':'.001','maxQty':'100','stepSize':'.001'},
                      {'filterType':'PRICE_FILTER','minPrice':'.01','maxPrice':'1000000','tickSize':'.01'}]}
        self.assertTrue(parse_rules(p)['enabled'])
        self.assertFalse(parse_rules({**p,'ocoAllowed':False})['enabled'])
        self.assertFalse(parse_rules({**p,'orderTypes':['MARKET','LIMIT_MAKER']})['enabled'])
    def test_protective_filter_failure_prevents_entry(self):
        b=book();b['rules']['raw_filters']=[{'filterType':'NOTIONAL','minNotional':'9','maxNotional':'10000'}]
        with self.assertRaises(Blocked):plan(self.store.state,settings(),account(),candidate(),b,fee(),time.time(),False)
    def test_failed_connection_restores_previous_credentials(self):
        v=Vault(self.path/'api.dpapi');old={'key':'old-key','secret':'old-secret','environment':'live'};v.update('binance',old);b=Binance(v)
        with patch.object(b,'request',side_effect=RemoteError('Binance',401,-2015)):
            with self.assertRaises(RemoteError):b.connect('a'*64,'b'*64,'live')
        self.assertEqual(v.read()['binance'],old)
    def test_missing_commission_cannot_be_assumed_zero(self):
        b=Binance(Mock())
        with patch.object(b,'request',return_value={}):
            with self.assertRaises(Blocked):b.commission('BTCUSDT')
    def test_signed_request_and_no_mutation_retry(self):
        vault=Vault(self.path/'api.dpapi');vault.update('binance',{'key':'test-key','secret':'test-secret','environment':'testnet'});b=Binance(vault);b.clock_at=time.time()
        def call(url,method,headers,body):
            q=parse_qs(body.decode());sig=q.pop('signature')[0];encoded=body.decode().split('&signature=')[0]
            self.assertEqual(sig,hmac.new(b'test-secret',encoded.encode(),hashlib.sha256).hexdigest());self.assertEqual(headers['X-MBX-APIKEY'],'test-key');raise RemoteError('Binance',504,unknown=True)
        with patch('aegis.binance.call',side_effect=call) as mock:
            with self.assertRaises(RemoteError):b.request('order','POST',symbol='BTCUSDT',side='BUY',type='MARKET',quantity='.1')
            self.assertEqual(mock.call_count,1)
    def test_unrelated_endpoints_are_not_available(self):
        b=Binance(Mock())
        for route in ['capital/withdraw/apply','margin/order','futures/order']:
            with self.assertRaises(Blocked):b.request(route,'POST')
    def test_valuation_preserves_unpriced_assets(self):
        a={'balances':[{'asset':'BTC','free':'.1','locked':'0'},{'asset':'MISSING','free':'3','locked':'0'}]}
        v=value_assets(a,[{'symbol':'BTCUSDT','bidPrice':'100','askPrice':'101'}]);self.assertEqual(D(v['value_usdt']),D(10));self.assertEqual(v['unpriced'],['MISSING']);self.assertIsNone(v['assets'][-1]['value_usdt'])
    def test_unfinished_candles_cannot_enter_strategy(self):
        rows=[[0,'100','102','99','101','5',299999,'500'],[300000,'101','103','100','102','5',599999,'510']]
        self.assertEqual(len(parse_candles(rows,400000)),1)
        unfinished=[{'close_time':time.time()+300}]*65
        with self.assertRaises(Blocked):evaluate('BTCUSDT',unfinished,100,'.01',STRATEGY,time.time())
    def test_model_cannot_modify_levels_or_average_down(self):
        c=candidate();p={'decision':'BUY','symbol':'BTCUSDT','proposed_entry':100,'stop_loss':99,'take_profit':103,'explanation':'test','invalidation':['Expired'],'expires_at':int(time.time()+30)}
        validate(p,c,time.time())
        with self.assertRaises(Blocked):validate({**p,'stop_loss':98},c,time.time())
        with self.assertRaises(Blocked):validate(p,{**c,'event':'one_r'},time.time())
    def test_one_worker_per_ledger(self):
        with process_lock(self.path/'worker.lock'):
            with self.assertRaises(RuntimeError):
                with process_lock(self.path/'worker.lock'):pass

class HTTPTests(unittest.TestCase):
    def test_disconnected_wallet_is_empty_and_cross_origin_mutation_blocked(self):
        from main import server
        with tempfile.TemporaryDirectory() as temp:
            e=Engine(Path(temp));http=server(e,0);port=http.server_address[1]
            # Handler captures the port; rebuild at an assigned free test port.
            http.server_close();http=server(e,port)
            thread=threading.Thread(target=http.serve_forever,daemon=True);thread.start();base=f'http://127.0.0.1:{port}'
            try:
                html=urlopen(base).read().decode();token=html.split('name="aegis-session" content="')[1].split('"')[0]
                def req(path,headers={},body=None):return urlopen(Request(base+path,headers=headers,data=body))
                with self.assertRaises(HTTPError) as bad:req('/api/state')
                self.assertEqual(bad.exception.code,403);bad.exception.close()
                h={'X-Aegis-Token':token};state=json.load(req('/api/state',h));self.assertIsNone(state['account']);self.assertIsNone(state['settings']['allocation']);self.assertFalse(state['trades']);self.assertFalse(state['intents']);self.assertIsNone(state['performance']['net_pnl'])
                with self.assertRaises(HTTPError) as bad:req('/api/emergency',{**h,'Origin':'https://foreign.invalid','Content-Type':'application/json'},b'{}')
                self.assertEqual(bad.exception.code,403);bad.exception.close();self.assertEqual(e.state['mode'],'PAUSED')
            finally:http.shutdown();http.server_close();thread.join();e.close();e.store.db.close()

if __name__=='__main__':unittest.main()
