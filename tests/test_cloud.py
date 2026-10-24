import copy,io,json,os,tempfile,time,unittest
from types import SimpleNamespace
from unittest.mock import Mock,patch
from cryptography.fernet import Fernet
from aegis.cloud_auth import Login
from aegis.cloud_runtime import build_runtime,enqueue,command_rows,view
from aegis.cloud_secrets import CloudVault
from aegis.cloud_store import sql,Row
from aegis.cloud_web import application
from aegis.intelligence.store import ResearchStore
from aegis.intelligence.paper import reference_book
from aegis.domain import D,Blocked
from aegis.market import parse_rules
from test_intelligence import metadata,proposed

class CloudTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.store=ResearchStore(self.tmp.name)
  self.store.db.execute('CREATE TABLE cloud_commands(id TEXT PRIMARY KEY,created_at REAL,status TEXT,body TEXT,result TEXT)')
  self.key=Fernet.generate_key().decode();self.s,self.sup=build_runtime(self.store,self.key)
 def tearDown(self):self.s.close();self.store.close();self.tmp.cleanup()
 def test_encrypted_credentials_survive_restart_without_plaintext_in_database(self):
  vault=CloudVault(self.store,self.key);vault.update('fixture',{'secret':'do-not-expose'})
  self.assertNotIn('do-not-expose',self.store.state('cloud_encrypted_credentials'))
  self.assertEqual(CloudVault(self.store,self.key).read()['fixture']['secret'],'do-not-expose')
 def test_commands_deduplicate_and_reject_reused_identity(self):
  a=enqueue(self.store,'control/pause',{'request_id':'fixture'});b=enqueue(self.store,'control/pause',{'request_id':'fixture'})
  self.assertEqual(a,b);self.assertEqual(len(command_rows(self.store)),1)
  with self.assertRaises(Blocked):enqueue(self.store,'control/stop',{'request_id':'fixture'})
 def test_pause_and_stop_persist_and_supervisor_never_rearms_them(self):
  for path in ('control/pause','control/stop'):
   enqueue(self.store,path,{})
   with patch.object(self.s.control,'start') as start:self.sup.tick();start.assert_not_called()
   self.assertIn(self.store.state('cloud_control')['desired'],('PAUSED','STOPPED'))
   self.assertEqual(command_rows(self.store)[0]['status'],'COMPLETED')
 def test_virtual_deposit_is_not_trading_profit_and_keeps_loss_limits(self):
  self.sup.handle('control/pause',{});self.s.spot.paper.state['daily_halted']=True
  self.sup.handle('wallet/add',{'amount':'25'})
  p=self.s.spot.paper;self.assertEqual(D(p.state['cash']),D(35));self.assertEqual(D(p.metrics()['net_total']),D(0))
  self.assertTrue(p.state['daily_halted']);self.assertEqual(p.settings['risk_per_trade'],.1)
 def test_deposit_cannot_touch_real_wallet_or_active_position(self):
  self.s.execution_mode='real'
  with self.assertRaises(Blocked):self.sup.handle('wallet/add',{'amount':'10'})
  self.s.execution_mode='virtual';self.s.paper_mode='RUNNING'
  with self.assertRaises(Blocked):self.sup.handle('wallet/add',{'amount':'10'})
 def test_real_start_requires_explicit_authorization_and_environment(self):
  with self.assertRaises(Blocked):self.sup.handle('control/start',{'mode':'real'})
  with self.assertRaises(Blocked):self.sup.handle('control/start',{'mode':'real','confirmation':'START REAL'})
  self.assertEqual(self.s.execution_mode,'virtual');self.assertFalse(self.sup.control['real_authorized'])
 def test_all_wallet_fills_visible_and_no_marker_for_a_wait(self):
  now=time.time();self.store.append('trade_episodes',{'kind':'ENTRY','wallet_key':self.s.spot.baseline.key,'filled_qty':'1','position':{'entry':'100'},'mode':'PAPER'},symbol='BTCUSDT',as_of=now)
  self.store.append('decisions',{'action':'WAIT','source':'spot-lab-forward','reason':'Fixture rejection'},symbol='BTCUSDT',as_of=now)
  result=view(self.s,self.sup)
  self.assertEqual(result['trade_episodes'][0]['body']['wallet_key'],self.s.spot.baseline.key)
  self.assertIn('comparison',result['chart_wallets']);self.assertEqual(result['worker']['auto_resume'],True)
  self.assertIsInstance(result['observations'],list)
 def test_cloud_page_login_and_csrf_block_controls_before_queueing(self):
  envvars={'AEGIS_USERNAME':'aegis','AEGIS_PASSWORD':'private-example-pass','AEGIS_SESSION_SECRET':'s'*40,'AEGIS_LOCAL_WORKSPACE':self.tmp.name}
  captured={}
  def start(status,headers):captured.update(status=status,headers=dict(headers))
  def call(path,method='GET',body=b'',cookie='',csrf=''):
   env={'PATH_INFO':path,'REQUEST_METHOD':method,'CONTENT_LENGTH':str(len(body)),'wsgi.input':io.BytesIO(body),'wsgi.url_scheme':'http','HTTP_HOST':'127.0.0.1','HTTP_COOKIE':cookie,'HTTP_X_AEGIS_TOKEN':csrf}
   return b''.join(application(env,start))
  with patch.dict(os.environ,envvars),patch('aegis.cloud_web.database',return_value=self.store):
   self.assertIn(b'Sign in',call('/'));call('/api/state');self.assertTrue(captured['status'].startswith('401'))
   call('/login','POST',b'username=aegis&password=private-example-pass');self.assertTrue(captured['status'].startswith('303'))
   cookie=captured['headers']['Set-Cookie'].split(';')[0]
   call('/api/control/stop','POST',b'{}',cookie);self.assertTrue(captured['status'].startswith('400'));self.assertEqual(command_rows(self.store),[])
   login=Login('aegis','private-example-pass','s'*40,False);token=cookie.split('=',1)[1]
   call('/api/control/stop','POST',b'{}',cookie,login.csrf(token));self.assertTrue(captured['status'].startswith('202'))
 def test_invalid_or_expired_session_is_rejected(self):
  login=Login('owner','private-example-pass','s'*40);token=login.token()
  self.assertEqual(login.valid({'Cookie':'aegis_owner='+token}),token)
  self.assertIsNone(login.valid({'Cookie':'aegis_owner='+token+'x'}))
  with patch('time.time',return_value=time.time()+50000):self.assertIsNone(login.valid({'Cookie':'aegis_owner='+token}))
 def test_postgres_sql_contract_uses_bigint_and_atomic_upserts(self):
  self.assertIn('ON CONFLICT DO NOTHING',sql('INSERT OR IGNORE INTO state VALUES(?,?)'))
  self.assertIn('ON CONFLICT(key)',sql('INSERT OR REPLACE INTO assessment_cache VALUES(?,?,?)'))
  self.assertEqual(sql('BEGIN IMMEDIATE'),'BEGIN');self.assertEqual(Row({'value':4})[0],4)
 def test_no_start_before_stable_recovered_feed_and_no_new_real_order_on_unknown(self):
  self.s.stream=SimpleNamespace(ready=Mock(),thread=SimpleNamespace(is_alive=lambda:True))
  self.s.control.fresh=Mock(return_value=False)
  with patch.object(self.s.control,'start') as start:self.sup.tick();start.assert_not_called()

class ExchangeFixture:
 def __init__(self):self.posts=[];self.qty='0';self.order=None;self.uncertain=False
 def request(self,route,method='GET',**p):
  if route=='account':return {'accountType':'SPOT','canTrade':True,'balances':[{'asset':'USDT','free':'10','locked':'0'},{'asset':'BTC','free':self.qty,'locked':'0'}]}
  if route=='order' and method=='POST':
   self.posts.append((route,p));qty=p['quantity'];self.qty=str(D(qty)*D('.999'))
   self.order={'orderId':77,'symbol':'BTCUSDT','status':'FILLED','executedQty':qty,'transactTime':int(time.time()*1000)}
   if self.uncertain:raise TimeoutError('Fixture: response lost after exchange acceptance')
   return self.order
  if route=='order' and method=='GET':
   if p.get('orderId') in (88,89):return {'status':'NEW','executedQty':'0','type':'STOP_LOSS' if p['orderId']==89 else 'LIMIT_MAKER','side':'SELL','origQty':self.qty,'stopPrice':'99.5','price':'102'}
   return self.order
  if route=='myTrades':
   if p.get('orderId') in (88,89):return []
   return [{'id':1,'orderId':77,'price':'100','qty':self.order['executedQty'],'commission':str(D(self.order['executedQty'])*D('.001')),'commissionAsset':'BTC','isBuyer':True,'time':int(time.time()*1000)}]
  if route=='orderList/oco':self.posts.append((route,p));return {'orderListId':5,'orders':[{'orderId':88},{'orderId':89}]}
  if route=='orderList':return {'orderListId':5,'listOrderStatus':'EXECUTING','orders':[{'orderId':88},{'orderId':89}]}
  raise AssertionError((route,method,p))

class LiveFlowTests(unittest.TestCase):
 setUp=CloudTests.setUp
 tearDown=CloudTests.tearDown
 def configure(self):
  b=self.sup.live;api=ExchangeFixture();b.api=SimpleNamespace(request=api.request,credentials=lambda:{'key':'fixture','environment':'live'},status=lambda:{'configured':True,'environment':'live'})
  b.broker.api=b.api;b.enabled=True;b.store.state['mode']='RUNNING';b.store.save(b.state)
  b.account={**api.request('account'),'synced_at':time.time(),'open_orders':[]};b.last_ok=time.time()
  b.fees['BTCUSDT']={'buy':'.001','sell':'.001','at':time.time()}
  now=time.time();book=reference_book('BTCUSDT',100,parse_rules(metadata()),now,0);b.markets['BTCUSDT']=book
  return b,api,book,proposed('1m',now),now
 def test_real_entry_uses_confirmed_fills_and_attaches_native_oco(self):
  b,api,book,c,now=self.configure();result=b.enter(c,book,now)
  self.assertTrue(result['accepted']);self.assertEqual([p[0] for p in api.posts],['order','orderList/oco'])
  self.assertEqual(b.state['position']['status'],'PROTECTED');self.assertEqual(b.metrics()['closed_trades'],0)
  rows=self.store.records('trade_episodes');self.assertEqual(len(rows),1);self.assertEqual(rows[0]['body']['mode'],'REAL');self.assertEqual(rows[0]['body']['exchange_order_id'],77)
  with self.assertRaises(Blocked):b.enter(c,book,now)
  self.assertEqual(len(api.posts),2)
 def test_lost_entry_response_is_reconciled_without_a_second_buy(self):
  b,api,book,c,now=self.configure();api.uncertain=True;result=b.enter(c,book,now)
  self.assertFalse(result['accepted']);self.assertEqual(len(api.posts),1);self.assertTrue(b.broker.unresolved())
  b.broker.reconcile();b._resolve_entry();b._manage_position()
  self.assertEqual([p[0] for p in api.posts],['order','orderList/oco']);self.assertEqual(b.state['position']['status'],'PROTECTED')
 def test_unvalued_third_asset_commission_never_becomes_exact_profit(self):
  b,api,book,c,now=self.configure()
  b.store.merge_trades('BTCUSDT',[{'id':99,'orderId':99,'price':'100','qty':'.07','commission':'.0001','commissionAsset':'BNB','isBuyer':True,'time':int(now*1000)}])
  self.assertIsNone(b.metrics()['net_total']);self.assertIsNone(b.metrics()['fees'])
 def test_paused_service_generation_blocks_real_dispatch_before_an_intent(self):
  b,api,book,c,now=self.configure()
  with self.assertRaises(Blocked):b.enter(c,book,now,expected_generation=self.s.generation)
  self.assertEqual(api.posts,[]);self.assertIsNone(b.state.get('pending_entry'))
