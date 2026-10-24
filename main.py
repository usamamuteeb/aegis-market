"""Local control plane. The trading worker does not depend on an open browser."""
import argparse
import json
import mimetypes
import secrets
import signal
import threading
import re
from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit, parse_qs
from aegis.config import ROOT, DATA, PORT
from aegis.domain import Blocked, fingerprint
from aegis.engine import Engine
from aegis.process_lock import process_lock
from aegis.transport import RemoteError
from aegis.auth_errors import public_failure


def server(engine, port=PORT):
    token = secrets.token_urlsafe(32)
    host = '127.0.0.1:' + str(port)
    dist = ROOT / 'frontend' / 'dist'
    auth_results = {}
    def auth_page(result):
        stylesheet = re.search(r'href="(/assets/[^\"]+\.css)"', (dist/'index.html').read_text(encoding='utf-8'))
        style = '<link rel="stylesheet" href="'+escape(stylesheet[1],quote=True)+'">' if stylesheet else ''
        title = result['title']
        diagnostic = ('<div class="auth-diagnostic">'+escape(result.get('code',''))+' · '+escape(result.get('stage',''))+'</div>') if result.get('code') else ''
        return ('<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
                '<title>'+escape(title)+' · Aegis</title>'+style+'</head><body class="auth-result"><article class="auth-card">'
                '<div class="auth-brand">AEGIS / CHATGPT</div><h1>'+escape(title)+'</h1><p>'+escape(result['message'])+'</p>'+diagnostic+
                '<a class="auth-return" href="/#connections">Return to Connections →</a></article></body></html>').encode('utf-8')
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args): pass  # OAuth codes, signed queries and credentials never enter request logs.

        def send(self, code, value, content_type='application/json'):
            data = json.dumps(value, allow_nan=False).encode() if content_type == 'application/json' else value
            self.send_response(code)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(data)))
            self.send_header('Cache-Control', 'no-store')
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.send_header('Referrer-Policy', 'no-referrer')
            self.send_header('Content-Security-Policy', "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
            self.end_headers()
            self.wfile.write(data)

        def allowed(self, api=False):
            if self.headers.get('Host') != host:
                self.send(403, {'error': 'Invalid host'}); return False
            origin = self.headers.get('Origin')
            if origin and origin != 'http://' + host:
                self.send(403, {'error': 'Invalid origin'}); return False
            if api and not secrets.compare_digest(self.headers.get('X-Aegis-Token', ''), token):
                self.send(403, {'error': 'Reload the local dashboard'}); return False
            return True

        def do_GET(self):
            path = urlsplit(self.path).path
            if not self.allowed(path.startswith('/api/')): return
            try:
                if path == '/auth/callback':
                    query = {k: v[0] for k, v in parse_qs(urlsplit(self.path).query).items()}
                    try:
                        status = engine.chat.callback(query)
                        engine.clear_model()
                        engine.pause('ChatGPT sign-in changed; entries paused')
                        engine.change(lambda s: s.update(generation=s['generation'] + 1))
                        engine.store.event('connection', 'ChatGPT authorization completed. Select a model in Connections.')
                        result = {'title': 'ChatGPT connected' if status['plan_enabled'] else 'Signed in · plan permission missing',
                                  'message': 'Return to Connections, refresh the available models and select your model. Trading remains paused.' if status['plan_enabled'] else 'Your identity was verified, but ChatGPT plan usage was not granted. Review the account and plan permission before reconnecting.'}
                    except Exception as exc:
                        failure = public_failure(exc, 'session_setup')
                        engine.store.event('connection', failure.message, error_code=failure.code, stage=failure.stage)
                        result = {'title':'Sign-in did not complete', **failure.public()}
                    rid = secrets.token_urlsafe(12)
                    auth_results[rid] = result
                    if len(auth_results)>20: auth_results.pop(next(iter(auth_results)))
                    # Remove the one-time code and OAuth state from the resulting page's address.
                    self.send_response(303)
                    self.send_header('Location', '/auth/result?r='+rid)
                    self.send_header('Content-Length', '0')
                    self.send_header('Cache-Control', 'no-store')
                    self.send_header('Referrer-Policy', 'no-referrer')
                    self.end_headers()
                    return
                if path == '/auth/result':
                    rid = parse_qs(urlsplit(self.path).query).get('r',[''])[0]
                    result = auth_results.get(rid, {'title':'Start a fresh sign-in', 'message':'Return to Connections and choose Continue with ChatGPT. Keep the Aegis worker running until authorization finishes.'})
                    return self.send(200, auth_page(result), 'text/html; charset=utf-8')
                if path == '/health':
                    return self.send(200, {'app':'aegis','instance':fingerprint(str(engine.report_path.parent.parent.resolve()).casefold())})
                if path == '/api/research/report':
                    if not engine.report_path.is_file(): raise Blocked('No historical screening report is available')
                    return self.send(200, json.loads(engine.report_path.read_text(encoding='utf-8')))
                if path == '/api/state': return self.send(200, engine.snapshot())
                if path == '/api/history':
                    q = parse_qs(urlsplit(self.path).query)
                    return self.send(200, engine.history(q.get('symbol', ['BTCUSDT'])[0], q.get('before', [None])[0]))
                if path == '/api/export':
                    return self.send(200, {'performance': engine.performance(), 'trades': engine.store.trades(limit=100000),
                                           'intents': engine.store.intents(), 'events': engine.store.events(100000)})
                if path == '/':
                    html = (dist / 'index.html').read_bytes().replace(b'__AEGIS_SESSION__', token.encode())
                    return self.send(200, html, 'text/html; charset=utf-8')
                if path.startswith('/assets/'):
                    target = (dist / path.lstrip('/')).resolve()
                    if target.is_relative_to(dist.resolve()) and target.is_file():
                        return self.send(200, target.read_bytes(), mimetypes.guess_type(target)[0] or 'application/octet-stream')
                self.send(404, {'error': 'Not found'})
            except (Blocked, RemoteError, ValueError) as exc: self.send(400, {'error': str(exc)})
            except Exception: self.send(503, {'error': 'Local service could not complete this request'})

        def do_POST(self):
            if not self.allowed(True): return
            try:
                n = int(self.headers.get('Content-Length', '0'))
                if not 0 < n <= 12000: raise Blocked('Invalid request size')
                if self.headers.get('Content-Type', '').split(';')[0] != 'application/json': raise Blocked('JSON required')
                body = json.loads(self.rfile.read(n))
                if not isinstance(body, dict): raise Blocked('JSON object required')
                path = urlsplit(self.path).path
                result = {'ok': True}
                if path == '/api/binance/connect': result = engine.connect_binance(body)
                elif path == '/api/binance/disconnect': engine.disconnect_binance()
                elif path == '/api/chatgpt/connect':
                    engine.pause('ChatGPT sign-in requested; entries paused')
                    result = {'url': engine.chat.begin(body.get('profile_id'), new_registration=body.get('new_registration') is True)}
                elif path == '/api/chatgpt/select':
                    engine.pause('ChatGPT account changed; entries paused')
                    engine.chat.select_account(body.get('profile_id'))
                    engine.clear_model()
                elif path == '/api/chatgpt/disconnect':
                    engine.pause('ChatGPT disconnected; entries paused')
                    result = engine.chat.disconnect()
                    engine.clear_model()
                elif path == '/api/chatgpt/models': result = {'models': engine.chat.models(refresh=True)}
                elif path == '/api/research/start': result = engine.start_research()
                elif path == '/api/shutdown':
                    if engine.state['mode']!='PAUSED' or engine.state['position'] or engine.state.get('pending_entry') or engine.broker.unresolved() or engine.inflight:
                        raise Blocked('Pause entries and resolve managed exposure/requests before stopping the worker')
                    engine.shutdown.set()
                    threading.Thread(target=http.shutdown,daemon=True).start()
                elif path == '/api/settings': engine.save_settings(body)
                elif path == '/api/arm': engine.arm(body.get('confirmation'))
                elif path == '/api/pause': engine.pause()
                elif path == '/api/emergency': engine.emergency()
                elif path == '/api/reset-emergency': engine.reset_emergency()
                elif path == '/api/exit':
                    if not engine.state['position']: raise Blocked('There is no managed position')
                    engine.change(lambda s: s['position'].update(exit_requested='User requested close'), ('control', {'message': 'Managed position exit requested'}))
                else: return self.send(404, {'error': 'Not found'})
                self.send(200, result)
            except (Blocked, RemoteError, ValueError, TypeError) as exc: self.send(400, {'error': str(exc)})
            except Exception: self.send(503, {'error': 'Local service could not complete this request'})
    http = ThreadingHTTPServer(('127.0.0.1', port), Handler)
    http.daemon_threads = True
    return http


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-dir', type=Path, default=DATA)
    parser.add_argument('--port', type=int, default=PORT)
    parser.add_argument('--open-browser', action='store_true')
    parser.add_argument('--stop-worker', action='store_true')
    args = parser.parse_args()
    if args.open_browser or args.stop_worker:
        from urllib.request import Request, urlopen
        base=f'http://127.0.0.1:{args.port}'
        try:
            with urlopen(base+'/health',timeout=2) as response: health=json.load(response)
        except Exception: health=None
        same=health and health.get('app')=='aegis' and health.get('instance')==fingerprint(str(args.data_dir.resolve()).casefold())
        if args.stop_worker:
            if not same: print('No matching Aegis worker is running.'); return
            with urlopen(base,timeout=3) as response: html=response.read().decode()
            token=re.search(r'name="aegis-session" content="([^\"]+)"',html).group(1)
            try:
                with urlopen(Request(base+'/api/shutdown',data=b'{}',headers={'X-Aegis-Token':token,'Content-Type':'application/json'}),timeout=5): pass
                print('Worker stop requested. Native exchange orders were not cancelled.')
            except Exception:
                print('Worker could not stop. Pause entries and resolve managed exposure first.')
            return
        if same:
            import webbrowser
            webbrowser.open(base);print('Existing Aegis worker is running; dashboard opened.');return
    lock = process_lock(args.data_dir / 'worker.lock')
    lock.__enter__()
    engine = Engine(args.data_dir)
    engine.chat.port = args.port
    http = server(engine, args.port)
    worker = threading.Thread(target=engine.run, daemon=True, name='trading-worker')
    worker.start()
    if args.open_browser:
        import webbrowser
        threading.Timer(.5, lambda: webbrowser.open(f'http://127.0.0.1:{args.port}')).start()
    print(f'Aegis: http://127.0.0.1:{args.port} | entries paused | keep this window running', flush=True)
    def stop(*_):
        engine.shutdown.set()
        threading.Thread(target=http.shutdown, daemon=True).start()
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    try: http.serve_forever(poll_interval=.2)
    finally:
        engine.close(); worker.join(timeout=12); http.server_close(); lock.__exit__(None, None, None)

if __name__ == '__main__': main()
