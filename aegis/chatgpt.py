"""Supported Sign in with ChatGPT, OIDC/PKCE and subscription Responses streaming."""
import base64
import hashlib
import json
import secrets
import threading
import time
import uuid
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import jwt
from aegis.domain import Blocked, fingerprint
from aegis.transport import call, RemoteError, open_request
from aegis.auth_errors import public_failure

AUTH = 'https://auth.openai.com'
RESOURCE = 'https://api.openai.com/v1'
SCOPES = 'openid profile email offline_access resource.invoke chatgpt.tokens.use.direct'
ID_TOKEN_CLOCK_LEEWAY = 60

class InferenceFailure(Blocked):
    def __init__(self,message,usage=None):
        super().__init__(message);self.usage=usage or {}


class ChatGPT:
    def __init__(self, vault, port):
        self.vault, self.port = vault, port
        self.lock = threading.RLock()
        self.pending = None
        self.catalog, self.catalog_at = [], 0
        self.error = None
        self.discovery = None

    def oidc(self):
        if not self.discovery:
            data = call(AUTH + '/.well-known/openid-configuration')
            from urllib.parse import urlparse
            for field in ('issuer', 'jwks_uri', 'token_endpoint', 'authorization_endpoint'):
                if urlparse(data[field]).scheme != 'https' or urlparse(data[field]).hostname != 'auth.openai.com':
                    raise Blocked('Unexpected OpenAI identity endpoint')
            self.discovery = data
        return self.discovery

    def saved(self):
        return self.vault.read().get('chatgpt', {'profiles': {}, 'active': None})

    def begin(self, profile_id=None, new_registration=False):
        with self.lock:
            host = self.vault.read().get('host_id')
            if not host:
                host = 'urn:uuid:' + str(uuid.uuid4())
                self.vault.update('host_id', host)
            saved = self.saved()
            old = saved['profiles'].get(profile_id) if profile_id else None
            verifier = secrets.token_urlsafe(48)
            state, nonce = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
            redirect = f'http://127.0.0.1:{self.port}/auth/callback'
            if old and new_registration: raise Blocked('Choose a new account registration instead of replacing a saved account')
            client = old['client_id'] if old else ('dynamic_agent_client' if new_registration else self.vault.read().get('last_issued_client', 'dynamic_agent_client'))
            params = {'client_id': client, 'ext_agent_host_id': host, 'response_type': 'code',
                      'redirect_uri': redirect, 'scope': SCOPES, 'resource': RESOURCE,
                      'state': state, 'nonce': nonce, 'code_challenge_method': 'S256',
                      'code_challenge': base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b'=').decode()}
            if client == 'dynamic_agent_client':
                params['agent_name_hint'] = 'Aegis Binance Spot'
            if old and old.get('id_token'):
                params['id_token_hint'] = old['id_token']
            if old and old.get('email'):
                params['login_hint'] = old['email']
            self.error = None
            self.vault.update('chatgpt_last_error', None)
            self.pending = {'state': state, 'nonce': nonce, 'verifier': verifier, 'redirect': redirect,
                            'client': client, 'old': old, 'created_at': time.time()}
            return AUTH + '/api/accounts/authorize?' + urlencode(params)

    def token_request(self, values):
        return call(AUTH + '/api/accounts/oauth/token', 'POST',
                    {'Content-Type': 'application/x-www-form-urlencoded'}, urlencode(values).encode())

    def callback(self, query):
        with self.lock:
            self.callback_stage = 'callback_validation'
            try:
                return self._callback(query)
            except Exception as exc:
                failure = public_failure(exc, self.callback_stage)
                self.error = failure.public()
                try: self.vault.update('chatgpt_last_error', self.error)
                except Exception: pass
                raise failure from None

    def _callback(self, query):
        with self.lock:
            attempt = self.pending
            if not attempt or time.time() - attempt['created_at'] > 600 or not secrets.compare_digest(query.get('state', ''), attempt['state']):
                raise Blocked('Invalid or expired sign-in callback')
            self.pending = None
            if query.get('error'):
                raise Blocked('ChatGPT authorization was not granted')
            issued = query.get('client_id') or (attempt['client'] if attempt['client'] != 'dynamic_agent_client' else '')
            if not issued or issued == 'dynamic_agent_client' or len(issued) > 300:
                raise Blocked('OpenAI did not return a valid registered client')
            if attempt['client'] != 'dynamic_agent_client' and issued != attempt['client']:
                raise Blocked('Sign-in registration mismatch')
            # Retain the issued registration before exchanging an expiring code.
            self.vault.update('last_issued_client', issued)
            self.callback_stage = 'token_exchange'
            tokens = self.token_request({'grant_type': 'authorization_code', 'client_id': issued,
                       'code': query.get('code', ''), 'code_verifier': attempt['verifier'],
                       'redirect_uri': attempt['redirect'], 'resource': RESOURCE})
            self.callback_stage = 'identity_discovery'
            discovery = self.oidc()
            id_token = tokens.get('id_token')
            if not id_token:
                raise Blocked('OpenAI identity token missing')
            if not tokens.get('access_token'):
                raise Blocked('OpenAI access token missing')
            self.callback_stage = 'identity_keys'
            signing = jwt.PyJWKClient(discovery['jwks_uri'], timeout=8).get_signing_key_from_jwt(id_token)
            self.callback_stage = 'identity_validation'
            claims = jwt.decode(id_token, signing.key, algorithms=['RS256', 'ES256'],
                                audience=issued, issuer=discovery['issuer'],
                                leeway=ID_TOKEN_CLOCK_LEEWAY,
                                options={'require': ['exp', 'iss', 'aud', 'sub', 'nonce']})
            if not secrets.compare_digest(str(claims['nonce']), attempt['nonce']):
                raise Blocked('Identity nonce mismatch')
            if attempt['old'] and claims['sub'] != attempt['old']['sub']:
                raise Blocked('Signed-in identity differs from selected account')
            pid = fingerprint({'issuer': claims['iss'], 'client_id': issued, 'sub': claims['sub']})[:24]
            profile = {'client_id': issued, 'sub': claims['sub'], 'email': claims.get('email', ''),
                       'name': claims.get('name', claims.get('email', 'ChatGPT account')),
                       'access_token': tokens.get('access_token'), 'refresh_token': tokens.get('refresh_token'),
                       'id_token': id_token, 'scopes': tokens.get('scope', '').split(),
                       'expires_at': time.time() + int(tokens.get('expires_in', 300))}
            saved = self.saved()
            saved['profiles'][pid] = profile
            saved['active'] = pid
            self.callback_stage = 'credential_storage'
            self.vault.update('chatgpt', saved)
            self.vault.update('last_issued_client', None)
            self.vault.update('chatgpt_last_error', None)
            self.catalog, self.catalog_at, self.error = [], 0, None
            return self.status()

    def active(self):
        data = self.saved()
        return data['profiles'].get(data.get('active'))

    def access_token(self):
        with self.lock:
            saved = self.saved()
            p = saved['profiles'].get(saved.get('active'))
            if not p or not p.get('access_token'):
                raise Blocked('Connect your ChatGPT account')
            if 'chatgpt.tokens.use.direct' not in p.get('scopes', []):
                raise Blocked('ChatGPT plan usage permission was not granted')
            if time.time() >= p['expires_at'] - 60:
                if not p.get('refresh_token'):
                    raise Blocked('ChatGPT session expired; sign in again')
                tokens = self.token_request({'grant_type': 'refresh_token', 'client_id': p['client_id'],
                                            'refresh_token': p['refresh_token'], 'resource': RESOURCE})
                p['access_token'] = tokens['access_token']
                p['refresh_token'] = tokens.get('refresh_token', p['refresh_token'])
                p['expires_at'] = time.time() + int(tokens.get('expires_in', 300))
                if 'scope' in tokens:
                    p['scopes'] = tokens['scope'].split()
                self.vault.update('chatgpt', saved)
                if 'chatgpt.tokens.use.direct' not in p['scopes']:
                    raise Blocked('ChatGPT plan permission no longer available')
            return p['access_token']

    def models(self, refresh=False):
        if refresh or time.time() - self.catalog_at > 300:
            data = call(RESOURCE + '/models', headers={'Authorization': 'Bearer ' + self.access_token()})
            self.catalog = [{'slug': m['slug'], 'name': m.get('display_name', m['slug'])}
                            for m in data.get('models', []) if m.get('visibility') == 'list']
            self.catalog_at = time.time()
        return self.catalog

    def select_account(self, profile_id):
        with self.lock:
            saved = self.saved()
            if profile_id not in saved['profiles']:
                raise Blocked('Unknown ChatGPT account')
            saved['active'] = profile_id
            self.vault.update('chatgpt', saved)
            self.catalog, self.catalog_at = [], 0

    def disconnect(self):
        with self.lock:
            saved = self.saved()
            p = saved['profiles'].get(saved.get('active'))
            confirmed = False
            if p and p.get('refresh_token'):
                try:
                    endpoint = self.oidc().get('revocation_endpoint')
                    if endpoint:
                        call(endpoint, 'POST', {'Content-Type': 'application/x-www-form-urlencoded'},
                             urlencode({'token': p['refresh_token'], 'token_type_hint': 'refresh_token', 'client_id': p['client_id']}).encode())
                        confirmed = True
                except Exception:
                    pass
            if p:
                for key in ('access_token', 'refresh_token', 'id_token'):
                    p.pop(key, None)
            saved['active'] = None
            self.vault.update('chatgpt', saved)
            self.catalog, self.catalog_at = [], 0
            return {'revocation_confirmed': confirmed}

    def status(self):
        s = self.saved()
        p = s['profiles'].get(s.get('active'))
        return {'connected': bool(p and p.get('access_token')), 'plan_enabled': bool(p and p.get('access_token') and 'chatgpt.tokens.use.direct' in p.get('scopes', [])),
                'active': s.get('active'), 'accounts': [{'id': k, 'name': v['name'], 'email': v['email']} for k, v in s['profiles'].items()],
                'models': self.catalog, 'error': self.error or self.vault.read().get('chatgpt_last_error'), 'billing': 'ChatGPT plan'}

    def assess(self, context, snapshot, model, schema, timeout=20):
        if model not in [m['slug'] for m in self.models()]:
            raise Blocked('Selected model is not available to this ChatGPT account')
        body = {'model': model, 'store': False, 'stream': True,
                'instructions': 'Assess only the supplied fixed Spot trend/pullback candidate or position event. Return BUY, WAIT or EXIT as structured JSON. BUY must exactly echo candidate levels. WAIT and EXIT use null levels. Never invent a strategy or bypass coded risk. Use integer Unix expiry no later than context expires_at. Give a brief reason and invalidation conditions.',
                'input': [{'role': 'user', 'content': json.dumps({'context': context, 'market': snapshot}, allow_nan=False)}],
                'text': {'format': {'type': 'json_schema', 'name': 'trade_assessment', 'strict': True, 'schema': schema}}}
        return self._infer(body,timeout)

    def assess_evidence(self, packet, model, schema, max_output_tokens=500, reasoning='low', timeout=20):
        if model not in [m['slug'] for m in self.models()]:
            raise Blocked('Selected GPT-6 Sol is unavailable to this account; WAIT')
        body = {'model': model, 'store': False, 'stream': True, 'max_output_tokens': max_output_tokens,
                'reasoning': {'effort': reasoning},
                'instructions': 'Evaluate only the supplied evidence. Return the strict assessment schema. Evidence probabilities belong to code. You may reject a candidate; never invent evidence, probabilities, funds, sizes or change protective levels. Cite only supplied evidence_ids. WAIT on uncertainty. Keep rationale concise.',
                'input': [{'role': 'user', 'content': json.dumps(packet,allow_nan=False)}],
                'text': {'format': {'type':'json_schema','name':'evidence_assessment','strict':True,'schema':schema}}}
        return self._infer(body,timeout)

    def _infer(self, body, timeout):
        req = Request(RESOURCE + '/responses', data=json.dumps(body).encode(),
                      headers={'Authorization': 'Bearer ' + self.access_token(), 'Content-Type': 'application/json', 'Accept': 'text/event-stream'})
        started, completed, output, usage = time.monotonic(), False, [], {}
        try:
            with open_request(req, timeout=timeout) as response:
                event_data = []
                for raw in response:
                    if time.monotonic() - started > timeout:
                        raise Blocked('ChatGPT assessment timed out; WAIT')
                    line = raw.decode('utf-8').strip()
                    if line.startswith('data:'):
                        event_data.append(line[5:].strip())
                    elif not line and event_data:
                        text = '\n'.join(event_data)
                        event_data = []
                        if text == '[DONE]':
                            continue
                        event = json.loads(text)
                        if event['type'] == 'response.output_text.delta':
                            output.append(event['delta'])
                            if sum(len(x) for x in output)>16000:raise InferenceFailure('Assessment output exceeded local bound; WAIT',usage)
                        elif event['type'] in ('response.failed', 'response.incomplete', 'error'):
                            raise InferenceFailure('ChatGPT declined, interrupted, or usage limit reached; WAIT',event.get('response',{}).get('usage'))
                        elif event['type'] == 'response.completed':
                            completed = True
                            usage = event.get('response', {}).get('usage', {})
                            if not output:
                                output = [c['text'] for o in event['response'].get('output', []) if o.get('type') == 'message'
                                          for c in o.get('content', []) if c.get('type') == 'output_text']
                if not completed:
                    raise Blocked('ChatGPT stream did not complete; WAIT')
        except Blocked:
            raise
        except Exception:
            raise Blocked('ChatGPT unavailable; WAIT') from None
        return json.loads(''.join(output)), usage
