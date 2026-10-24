"""Multi-service boundary: login, internal document fetch, original API paths."""
import io,json,os,re,unittest
from email.message import Message
from pathlib import Path
from unittest.mock import Mock,patch
from aegis.cloud_auth import Login
from aegis.cloud_web import application,dashboard_document

DOCUMENT='<html><head><meta name="aegis-session" content="__AEGIS_SESSION__"></head><body><script src="/assets/app.js"></script></body></html>'

class ServiceRoutingTests(unittest.TestCase):
 def setUp(self):
  self.vars={'AEGIS_USERNAME':'aegis','AEGIS_PASSWORD':'private-example-pass','AEGIS_SESSION_SECRET':'s'*40,'VERCEL':'1','AEGIS_FRONTEND_URL':'https://frontend.internal/ui/'}
  self.login=Login('aegis','private-example-pass','s'*40)
  self.cookie='aegis_owner='+self.login.token()
  self.headers=Message();self.headers['Content-Type']='text/html; charset=utf-8'
 def call(self,path,cookie='',query=''):
  result={}
  def start(status,headers):result.update(status=status,headers=dict(headers))
  body=b''.join(application({'PATH_INFO':path,'REQUEST_METHOD':'GET','QUERY_STRING':query,'HTTP_COOKIE':cookie},start))
  return result,body
 def response(self,document=DOCUMENT,status=200):
  response=Mock(status=status,headers=self.headers)
  response.read.return_value=document.encode()
  response.__enter__=Mock(return_value=response);response.__exit__=Mock(return_value=False)
  return response
 def test_login_does_not_fetch_private_frontend(self):
  with patch.dict(os.environ,self.vars),patch('aegis.cloud_web._frontend_opener.open') as fetch:
   result,body=self.call('/')
  self.assertEqual(result['status'],'200 OK');self.assertIn(b'Sign in',body);fetch.assert_not_called()
 def test_authenticated_dashboard_uses_runtime_binding_and_injects_session(self):
  with patch.dict(os.environ,self.vars),patch('aegis.cloud_web._frontend_opener.open',return_value=self.response()) as fetch:
   result,body=self.call('/',self.cookie)
  self.assertEqual(result['status'],'200 OK')
  self.assertIn(b'aegis-cloud',body);self.assertNotIn(b'__AEGIS_SESSION__',body)
  self.assertIn(b'/assets/app.js',body)
  request=fetch.call_args.args[0]
  self.assertEqual(request.full_url,'https://frontend.internal/ui/')
  self.assertFalse(request.has_header('Cookie'));self.assertFalse(request.has_header('X-aegis-token'))
 def test_missing_binding_in_vercel_never_falls_back_to_another_services_build(self):
  with patch.dict(os.environ,{**self.vars,'AEGIS_FRONTEND_URL':''}),patch('aegis.cloud_web.ROOT') as root:
   result,body=self.call('/',self.cookie)
  self.assertEqual(result['status'],'503 Service Unavailable');root.__truediv__.assert_not_called()
 def test_bad_or_unavailable_frontend_never_returns_partial_dashboard(self):
  for document,status in (('not html',200),(DOCUMENT,404),('x'*1048577,200)):
   with self.subTest(status=status,size=len(document)),patch.dict(os.environ,self.vars),patch('aegis.cloud_web._frontend_opener.open',return_value=self.response(document,status)):
    result,body=self.call('/',self.cookie)
    self.assertEqual(result['status'],'503 Service Unavailable');self.assertNotIn(b'aegis-session',body)
  with patch.dict(os.environ,self.vars),patch('aegis.cloud_web._frontend_opener.open',side_effect=TimeoutError):
   result,_=self.call('/',self.cookie)
   self.assertEqual(result['status'],'503 Service Unavailable')
 def test_invalid_binding_is_rejected_before_network(self):
  for base in ('file:///secret','https://user:pass@host/','https://host/?secret=1'):
   with self.subTest(base=base),patch.dict(os.environ,{**self.vars,'AEGIS_FRONTEND_URL':base}),patch('aegis.cloud_web._frontend_opener.open') as fetch:
    with self.assertRaises(RuntimeError):dashboard_document()
    fetch.assert_not_called()
 def test_local_single_process_preview_can_still_use_local_build(self):
  with patch.dict(os.environ,{'AEGIS_FRONTEND_URL':'','VERCEL':''}),patch('aegis.cloud_web.ROOT') as root:
   root.__truediv__.return_value.read_text.return_value=DOCUMENT
   self.assertEqual(dashboard_document(),DOCUMENT)
 def test_original_api_prefix_survives_services_routing(self):
  store=Mock();store.state.return_value=None
  with patch.dict(os.environ,self.vars),patch('aegis.cloud_web.database',return_value=store),patch('aegis.cloud_web.command_rows',return_value=[]),patch('aegis.cloud_web._frontend_opener.open') as fetch:
   for path,query in (('/api/state',''),('/api/index.py','route=state')):
    result,body=self.call(path,self.cookie,query)
    self.assertEqual(result['status'],'200 OK');self.assertTrue(json.loads(body)['initializing'])
   fetch.assert_not_called()
 def test_route_table_keeps_ui_document_gated_and_assets_on_static_service(self):
  config=json.loads((Path(__file__).resolve().parent.parent/'vercel.json').read_text())
  for path,expected in (('/','app'),('/login','app'),('/logout','app'),('/api/state','app'),('/api/control/start','app'),('/assets/app.js','frontend'),('/index.html','app')):
   target=next(rule['destination']['service'] for rule in config['rewrites'] if re.fullmatch(rule['source'],path))
   self.assertEqual(target,expected,path)
