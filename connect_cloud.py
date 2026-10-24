"""One-time official loopback ChatGPT sign-in for a named cloud host. Never prints tokens."""
import argparse,copy,getpass,http.cookiejar,json,re,secrets,threading,time,webbrowser
from http.server import BaseHTTPRequestHandler,HTTPServer
from urllib.parse import urlencode,parse_qs,urlsplit
from urllib.request import Request,build_opener,HTTPCookieProcessor
from aegis.chatgpt import ChatGPT
from aegis.domain import Blocked

class MemoryVault:
    def __init__(self,host):self.data={'host_id':host};self.lock=threading.RLock()
    def read(self):
        with self.lock:return copy.deepcopy(self.data)
    def update(self,key,value):
        with self.lock:self.data[key]=copy.deepcopy(value)

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--url');parser.add_argument('--import-binance',action='store_true');args=parser.parse_args()
    base=(args.url or input('Your Aegis Vercel URL: ')).rstrip('/');parsed=urlsplit(base)
    if parsed.scheme!='https' or not parsed.hostname or parsed.username or parsed.query or parsed.fragment:raise SystemExit('Use the exact HTTPS URL of your own deployment')
    print('Connecting only to '+base)
    username=input('Aegis username [aegis]: ') or 'aegis';password=getpass.getpass('Aegis password: ')
    opener=build_opener(HTTPCookieProcessor(http.cookiejar.CookieJar()))
    with opener.open(Request(base+'/login',data=urlencode({'username':username,'password':password}).encode()),timeout=15) as response:html=response.read().decode()
    match=re.search(r'name="aegis-session" content="([^"]+)"',html)
    if not match:raise SystemExit('Aegis login did not complete')
    token=match.group(1)
    def post(route,body):
        with opener.open(Request(base+'/api/'+route,data=json.dumps(body).encode(),headers={'Content-Type':'application/json','X-Aegis-Token':token}),timeout=40) as response:return json.load(response)
    host=post('setup/host',{})['host_id'];vault=MemoryVault(host);finished=threading.Event();failure=[]
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*a):pass
        def do_GET(self):
            if urlsplit(self.path).path!='/auth/callback':self.send_error(404);return
            try:
                chat.callback({k:v[0] for k,v in parse_qs(urlsplit(self.path).query).items()})
                saved=chat.saved();pid=saved['active'];post('model/import',{'active':pid,'profile':saved['profiles'][pid]})
                text='ChatGPT connected to your Aegis cloud worker. You can close this tab.'
            except Exception:
                failure.append(True);text='Connection did not complete. Close this tab and retry the helper.'
            payload=text.encode();self.send_response(200);self.send_header('Content-Type','text/plain');self.send_header('Content-Length',str(len(payload)));self.end_headers();self.wfile.write(payload);finished.set()
    server=HTTPServer(('127.0.0.1',0),Handler);chat=ChatGPT(vault,server.server_address[1])
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start();webbrowser.open(chat.begin())
    if not finished.wait(600):failure.append(True)
    server.shutdown();server.server_close()
    if failure:raise SystemExit('ChatGPT connection incomplete. No tokens were printed or written to a local file.')
    print('ChatGPT connected. No inference was requested by this helper.')
    if args.import_binance:
        from pathlib import Path
        from aegis.vault import Vault
        source=Path.home()/'Music/Aegis-Crypto-Agent/binance-v3/data/credentials.dpapi'
        existing=Vault(source).read().get('binance')
        if not existing:raise SystemExit('No existing local Binance credentials were found')
        if existing.get('environment')!='live':raise SystemExit('Existing account is not Binance.com live')
        post('binance/connect',{'key':existing['key'],'secret':existing['secret']})
        print('Existing Binance account connected to the named cloud host. No exchange orders were placed.')
    print('Open '+base+' to view status. Real trading requires your START REAL action.')

if __name__=='__main__':main()
