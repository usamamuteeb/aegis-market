import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from urllib.parse import urlencode, urlsplit
import threading
import jwt
from cryptography.hazmat.primitives.asymmetric import rsa
from aegis.chatgpt import ChatGPT, SCOPES
from aegis.vault import Vault
from aegis.auth_errors import SignInFailure, public_failure
from aegis.transport import RemoteError


class ClockSkewTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.chat = ChatGPT(Vault(Path(self.temp.name)/'test.dpapi'), 8765)
        self.key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    def complete(self, offset=12, expiry=300):
        self.chat.begin()
        state, nonce = self.chat.pending['state'], self.chat.pending['nonce']
        now = int(time.time())
        claims = {'iss':'https://auth.openai.com', 'aud':'issued-client', 'sub':'test-identity',
                  'iat':now+offset, 'nbf':now+offset, 'exp':now+expiry, 'nonce':nonce}
        token = jwt.encode(claims, self.key, algorithm='RS256', headers={'kid':'test-key'})
        tokens = {'id_token':token, 'access_token':'fixture-access', 'refresh_token':'fixture-refresh',
                  'scope':SCOPES, 'expires_in':3600}
        self.chat.discovery = {'issuer':'https://auth.openai.com','jwks_uri':'https://auth.openai.com/.well-known/jwks.json'}
        with patch.object(self.chat, 'token_request', return_value=tokens), patch('aegis.chatgpt.jwt.PyJWKClient') as jwks:
            jwks.return_value.get_signing_key_from_jwt.return_value = Mock(key=self.key.public_key())
            return self.chat.callback({'state':state,'code':'fixture-code','client_id':'issued-client'})

    def test_fresh_signed_identity_with_twelve_seconds_clock_skew_is_accepted(self):
        status = self.complete(offset=12)
        self.assertTrue(status['plan_enabled'])
        self.assertNotIn('fixture-access', json.dumps(status))

    def test_large_future_time_and_long_expired_identity_remain_rejected(self):
        for offset,expiry in ((300,3600),(0,-300)):
            with self.subTest(offset=offset,expiry=expiry):
                with self.assertRaises(SignInFailure) as error:self.complete(offset,expiry)
                self.assertEqual(error.exception.code, 'IDENTITY_CLOCK' if offset else 'IDENTITY_EXPIRED')
                self.assertFalse(self.chat.status()['connected'])

    def test_identity_error_does_not_echo_provider_or_token_text(self):
        error=public_failure(jwt.InvalidTokenError('DO-NOT-EXPOSE.fixture-token'),'identity_validation')
        self.assertNotIn('DO-NOT-EXPOSE',json.dumps(error.public()))
        self.assertEqual(error.code,'IDENTITY_INVALID')

    def test_registered_client_retry_does_not_create_another_registration(self):
        self.chat.vault.update('last_issued_client','retained-client')
        from urllib.parse import parse_qs
        self.assertEqual(parse_qs(urlsplit(self.chat.begin()).query)['client_id'],['retained-client'])
        self.assertEqual(parse_qs(urlsplit(self.chat.begin(new_registration=True)).query)['client_id'],['dynamic_agent_client'])

    def test_failed_callback_retains_specific_diagnostic_across_restart(self):
        self.chat.begin()
        with patch.object(self.chat,'token_request',side_effect=RemoteError('ChatGPT',400,'invalid_grant')):
            with self.assertRaises(SignInFailure) as failure:
                self.chat.callback({'state':self.chat.pending['state'],'client_id':'registered-client','code':'DO-NOT-EXPOSE.code'})
        self.assertEqual(failure.exception.code,'CODE_EXPIRED')
        restored=ChatGPT(self.chat.vault,8765)
        self.assertEqual(restored.status()['error']['stage'],'token_exchange')
        self.assertNotIn('DO-NOT-EXPOSE',json.dumps(restored.status()))
        restored.begin()
        self.assertIsNone(restored.status()['error'])


class TransportDiagnosticsTests(unittest.TestCase):
    def test_oauth_string_error_is_extracted_without_reflecting_description(self):
        import io
        from aegis.transport import call
        for raw,expected in [({'error':'invalid_grant','error_description':'DO-NOT-EXPOSE'},'invalid_grant'),
                             ({'error':{'code':'invalid_client','message':'DO-NOT-EXPOSE'}},'invalid_client'),
                             ({'error':'DO-NOT-EXPOSE'},None)]:
            error=HTTPError('https://auth.openai.com/api/accounts/oauth/token',400,'error',{},io.BytesIO(json.dumps(raw).encode()))
            with self.subTest(expected=expected),patch('aegis.transport.open_request',side_effect=error):
                with self.assertRaises(RemoteError) as raised:call('https://auth.openai.com/api/accounts/oauth/token','POST',body=b'fixture')
                self.assertEqual(raised.exception.code,expected)
                self.assertNotIn('DO-NOT-EXPOSE',str(raised.exception))


class CallbackPageTests(unittest.TestCase):
    def test_failed_code_exchange_redirects_to_sanitized_useful_page(self):
        from main import server
        from aegis.engine import Engine
        with tempfile.TemporaryDirectory() as temp:
            engine=Engine(Path(temp))
            probe=server(engine,0);port=probe.server_address[1];probe.server_close()
            http=server(engine,port);thread=threading.Thread(target=http.serve_forever,daemon=True);thread.start()
            engine.chat.port=port;engine.chat.begin()
            query=urlencode({'state':engine.chat.pending['state'],'client_id':'registered-client','code':'DO-NOT-EXPOSE.code'})
            try:
                with patch.object(engine.chat,'token_request',side_effect=RemoteError('ChatGPT',400,'invalid_grant')):
                    with urlopen(f'http://127.0.0.1:{port}/auth/callback?'+query,timeout=5) as response:
                        body=response.read().decode();location=response.url
                self.assertEqual(urlsplit(location).path,'/auth/result')
                self.assertNotIn('code=',urlsplit(location).query)
                self.assertIn('CODE_EXPIRED',body);self.assertIn('token_exchange',body)
                self.assertIn('href="/#connections"',body)
                self.assertNotIn('DO-NOT-EXPOSE',body+json.dumps(engine.store.events()))
                self.assertIsNone(engine.chat.active());self.assertEqual(engine.state['mode'],'PAUSED')
            finally:
                http.shutdown();http.server_close();thread.join();engine.close();engine.store.db.close()

if __name__=='__main__':unittest.main()
