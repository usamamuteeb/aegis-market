import json
import time
from urllib.request import Request, build_opener, HTTPRedirectHandler
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse

class RemoteError(RuntimeError):
    def __init__(self, service, status=0, code=None, unknown=False, retry_after=0):
        self.status, self.code, self.unknown, self.retry_after = status, code, unknown, retry_after
        text = f'{service} request failed'
        if code is not None:
            text += f' (code {code})'
        if unknown:
            text += '; execution status unknown, reconciliation required'
        super().__init__(text)

ALLOWED = {'api.binance.com', 'testnet.binance.vision', 'auth.openai.com', 'api.openai.com'}

class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None

OPENER = build_opener(NoRedirect)

def open_request(req, timeout=8):
    return OPENER.open(req, timeout=timeout)

def call(url, method='GET', headers=None, body=None, timeout=8):
    if urlparse(url).scheme != 'https' or urlparse(url).hostname not in ALLOWED:
        raise ValueError('Unapproved service endpoint')
    req = Request(url, method=method, headers={'User-Agent': 'Aegis/3.0', **(headers or {})}, data=body)
    service = 'Binance' if 'binance' in urlparse(url).hostname else 'ChatGPT'
    try:
        with open_request(req, timeout=timeout) as r:
            raw = r.read()
            return json.loads(raw) if raw else {}
    except HTTPError as exc:
        try:
            data = json.loads(exc.read())
            error = data.get('error')
            code = data.get('code') or (error.get('code') if isinstance(error, dict) else error)
            # OAuth errors can be strings. Do not copy arbitrary provider text into diagnostics.
            if not isinstance(code, int) and code not in {'invalid_grant', 'invalid_client', 'invalid_scope', 'access_denied', 'server_error', 'temporarily_unavailable', 'unsupported_grant_type'}:
                code = None
        except Exception:
            code = None
        exc.close()
        raise RemoteError(service, exc.code, code,
                          unknown=method in ('POST', 'DELETE') and (exc.code >= 500 or 300 <= exc.code < 400 or code in (-1006, -1007)),
                          retry_after=int(exc.headers.get('Retry-After', 0) or 0)) from None
    except (URLError, TimeoutError, OSError, ValueError):
        raise RemoteError(service, unknown=method in ('POST', 'DELETE')) from None
