"""Loopback isolated paper dashboard, no import of Engine, Vault or Binance."""
import json
import mimetypes
import secrets
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit,parse_qs
from aegis.domain import Blocked
from aegis.intelligence.store import ResearchStore
from aegis.intelligence.service import IntelligenceService
from aegis.intelligence.stream import PublicStream
from aegis.intelligence.control import ConsoleControl

def server(service,port=8777):
    if not getattr(service,'control',None):service.control=ConsoleControl(service)
    if service.spot and not getattr(service,'connection',None):
        from aegis.intelligence.connection import PlanConnection
        service.connection=PlanConnection(service,port)
        service.connection.restore()
    token=secrets.token_urlsafe(32);dist=Path(__file__).resolve().parent/'frontend'/'dist'
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def send(self,status,body,kind='application/json'):
            data=json.dumps(body,allow_nan=False).encode() if isinstance(body,(dict,list)) else body
            self.send_response(status);self.send_header('Content-Type',kind);self.send_header('Content-Length',str(len(data)))
            self.send_header('Cache-Control','no-store');self.send_header('X-Content-Type-Options','nosniff')
            self.send_header('Content-Security-Policy',"default-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'")
            try:self.end_headers();self.wfile.write(data)
            except (BrokenPipeError,ConnectionResetError,ConnectionAbortedError):pass
        def allowed(self,mutation=False):
            host=self.headers.get('Host','')
            if host not in (f'127.0.0.1:{self.server.server_address[1]}',f'localhost:{self.server.server_address[1]}'):self.send(403,{'error':'Loopback host required'});return False
            if self.path.startswith('/api/') and not secrets.compare_digest(self.headers.get('X-Aegis-Token',''),token):self.send(403,{'error':'Local session required'});return False
            origin=self.headers.get('Origin')
            if mutation and origin and origin not in ('http://'+host,):self.send(403,{'error':'Foreign origin blocked'});return False
            return True
        def do_GET(self):
            if not self.allowed():return
            path=urlsplit(self.path).path
            if path=='/api/intelligence':return self.send(200,{**service.snapshot(),'console':service.control.snapshot(),
                'chatgpt_connection':service.connection.status() if getattr(service,'connection',None) else None})
            if path=='/auth/callback' and getattr(service,'connection',None):
                try:
                    query={k:v[0] for k,v in parse_qs(urlsplit(self.path).query).items()}
                    service.connection.chat.callback(query);service.connection.catalog()
                    self.send_response(303);self.send_header('Location','/');self.send_header('Cache-Control','no-store');self.end_headers()
                except Exception:
                    self.send(400,b'<h1>ChatGPT connection did not complete</h1><p>Return to Aegis and start a fresh connection attempt.</p><a href="/">Return to dashboard</a>','text/html; charset=utf-8')
                return
            if path=='/api/report':return self.send(200,{'evaluation_runs':service.store.records('evaluation_runs',limit=100),'mode':'ISOLATED_PAPER','live_eligible':False})
            if path=='/':
                html=(dist/'index.html').read_bytes().replace(b'__AEGIS_SESSION__',token.encode()).replace(b'</head>',b'<meta name="aegis-paper-workspace" content="isolated"></head>')
                html=html.replace('<title>Aegis · Binance Spot</title>'.encode(),'<title>Aegis · Paper Intelligence</title>'.encode())
                return self.send(200,html,'text/html; charset=utf-8')
            if path.startswith('/assets/'):
                p=(dist/path.lstrip('/')).resolve()
                if p.is_relative_to(dist.resolve()) and p.is_file():return self.send(200,p.read_bytes(),mimetypes.guess_type(p)[0] or 'application/octet-stream')
            self.send(404,{'error':'Unknown isolated-paper route'})
        def do_POST(self):
            if not self.allowed(True):return
            try:
                n=int(self.headers.get('Content-Length','0'))
                if not 0<n<=2000 or self.headers.get('Content-Type','').split(';')[0]!='application/json':raise ValueError('Bounded JSON required')
                body=json.loads(self.rfile.read(n));path=urlsplit(self.path).path
                if not isinstance(body,dict):raise ValueError('JSON object required')
                if path.startswith('/api/model/'):
                    conn=getattr(service,'connection',None)
                    if not conn:raise ValueError('Model connection is unavailable')
                    if path=='/api/model/connect':
                        conn.disable();return self.send(200,{'url':conn.chat.begin()})
                    if path=='/api/model/saved':return self.send(200,conn.saved())
                    if path=='/api/model/models':return self.send(200,conn.catalog())
                    if path=='/api/model/select':return self.send(200,conn.select(body.get('model')))
                    if path=='/api/model/disable':conn.disable()
                    else:raise ValueError('Unknown model route')
                elif path=='/api/control/start':service.control.start(body.get('mode','paper'))
                elif path=='/api/control/pause':service.control.pause()
                elif path=='/api/control/stop':service.control.pause(emergency=True)
                elif path=='/api/research':
                    if service.control.snapshot()['busy']:raise ValueError('Automatic startup is already preparing research')
                    if service.job['running']:raise ValueError('A research job is already running')
                    symbols=body.get('symbols',['BTCUSDT']);intervals=body.get('intervals',['1m','5m']);days=int(body.get('days',14))
                    if not isinstance(symbols,list) or not symbols or len(symbols)>10 or any(not isinstance(s,str) or not s.isalnum() or not s.endswith('USDT') for s in symbols) or intervals not in (['1m'],['5m'],['1m','5m']) or not 1<=days<=730:raise ValueError('Invalid research window')
                    service.job={'running':True,'message':'Public research queued','error':None}
                    def job():
                        try:service.research(symbols,intervals,days)
                        except Exception as exc:service.job['error']=str(exc);service.job['message']='Research incomplete'
                        finally:
                            service.job['running']=False;service.store.put_state('last_research_job',service.job)
                    service.research_thread=threading.Thread(target=job,daemon=True);service.research_thread.start()
                elif path=='/api/feed/start':
                    if not service.prepared:raise ValueError('Complete research ingestion before starting the prospective stream')
                    if not getattr(service,'stream',None):
                        service.stream=PublicStream(service,sorted({s for s,i in service.prepared}),sorted({i for s,i in service.prepared}));service.stream.start()
                elif path=='/api/paper/start':
                    if not service.prepared or not service.last_feed or any(time.time()-q['received_at']>3 for q in service.last_feed.values()):raise ValueError('Resynchronized fresh public stream required')
                    service.paper_mode='RUNNING'
                elif path=='/api/paper/pause':service.pause()
                else:raise ValueError('No live/account/model activation route exists in this workspace')
                self.send(200,{'ok':True,'mode':'PAPER','live_eligible':False})
            except (ValueError,TypeError,Blocked) as exc:self.send(400,{'error':str(exc)})
            except Exception:self.send(503,{'error':'Isolated paper request failed'})
    class LocalServer(ThreadingHTTPServer):
        allow_reuse_address=False
        def server_bind(self):
            if hasattr(socket,'SO_EXCLUSIVEADDRUSE'):
                self.socket.setsockopt(socket.SOL_SOCKET,socket.SO_EXCLUSIVEADDRUSE,1)
            super().server_bind()
    return LocalServer(('127.0.0.1',port),Handler)

def run(directory,port=8777,*,open_browser=False):
    if port==8765:raise ValueError('The production port is reserved; use a separate port')
    from aegis.process_lock import process_lock
    store=ResearchStore(directory);lock=process_lock(store.directory/"paper-worker.lock");lock.__enter__()
    service=IntelligenceService(store,spot_lab=True);http=server(service,port)
    def worker():
        service.job={'running':True,'message':'Restoring frozen research artifacts; no model calls','error':None}
        try:
            service.restore()
            if service.spot:service.spot.resume_exits()
        except Exception as exc:service.last_error='Research restore incomplete: '+str(exc)
        finally:
            service.job['running']=False
            service.job['message']='Research artifacts ready; live use remains disabled' if not service.last_error else 'Research restore incomplete; inspect the error'
        while not service.stop.wait(.25):
            try:service.dispatch_pending()
            except Exception:
                service.pause();service.last_error='Paper worker encountered an error; entries paused. Inspect the activity log.'
    thread=threading.Thread(target=worker,daemon=True);thread.start()
    if open_browser:
        import webbrowser
        webbrowser.open(f'http://127.0.0.1:{http.server_address[1]}')
    print(f'Spot paper dashboard: http://127.0.0.1:{http.server_address[1]} | no exchange account/orders | GPT candidate assessment requires connection',flush=True)
    try:http.serve_forever(poll_interval=.2)
    except KeyboardInterrupt:pass
    finally:
        service.stop.set()
        service.control.close()
        if getattr(service,'stream',None):service.stream.close()
        if getattr(service,'research_thread',None):service.research_thread.join(timeout=30)
        thread.join(timeout=3);service.close();http.server_close();store.close();lock.__exit__(None,None,None)
