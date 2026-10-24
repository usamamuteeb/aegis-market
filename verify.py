"""One batched isolated acceptance run; external networking and production DB forbidden."""
import ipaddress,json,socket,sqlite3,sys,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parent;APP=ROOT
(ROOT/'verification').mkdir(exist_ok=True)
sys.path.insert(0,str(ROOT/'optional-local-libs'))
sys.path.insert(0,str(APP));sys.path.insert(0,str(APP/'tests'))
connect=socket.socket.connect;connect_ex=socket.socket.connect_ex;dbconnect=sqlite3.connect;external=[]
def permitted(address):
 try:return ipaddress.ip_address(address[0]).is_loopback
 except (ValueError,TypeError):return address[0]=='localhost'
def network(self,address):
 if not permitted(address):external.append(str(address[0]));raise AssertionError('External networking forbidden during tests')
 return connect(self,address)
def network_ex(self,address):
 if not permitted(address):external.append(str(address[0]));raise AssertionError('External networking forbidden during tests')
 return connect_ex(self,address)
def database(path,*a,**kw):
 if 'music' in str(path).lower() or 'binance-v3' in str(path).lower():raise AssertionError('Production database forbidden during tests')
 return dbconnect(path,*a,**kw)
socket.socket.connect=network;socket.socket.connect_ex=network_ex;sqlite3.connect=database
suite=unittest.defaultTestLoader.discover(str(APP/'tests'))
with (ROOT/'verification/tests.txt').open('w',encoding='utf-8') as f:
 result=unittest.TextTestRunner(stream=f,verbosity=2).run(suite)
report={'tests':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),'passed':result.wasSuccessful(),
 'external_connection_attempts':external,'real_orders':0,'provider_inference_requests':0,'production_db_access_allowed':False}
(ROOT/'verification/tests.json').write_text(json.dumps(report,indent=2))
print(json.dumps(report,indent=2));raise SystemExit(0 if result.wasSuccessful() else 1)
