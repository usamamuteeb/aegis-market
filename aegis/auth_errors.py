"""Public authentication diagnostics. Never reflect provider bodies, URLs or JWTs."""
import jwt
from aegis.domain import Blocked
from aegis.transport import RemoteError


class SignInFailure(Blocked):
    def __init__(self, code, message, stage):
        self.code, self.message, self.stage = code, message, stage
        super().__init__(message)

    def public(self):
        return {'code': self.code, 'message': self.message, 'stage': self.stage}


def public_failure(exc, stage='callback'):
    if isinstance(exc, SignInFailure): return exc
    blocked = {
        'Invalid or expired sign-in callback': ('SIGNIN_EXPIRED', 'This sign-in attempt expired, was replaced, or the app restarted. Return to Connections and start a fresh sign-in. Do not refresh the old callback.'),
        'ChatGPT authorization was not granted': ('AUTHORIZATION_DECLINED', 'OpenAI did not grant authorization. Start again and review the ChatGPT plan-usage permission.'),
        'OpenAI did not return a valid registered client': ('REGISTRATION_INCOMPLETE', 'OpenAI did not return the registered client ID. Start a fresh sign-in from Connections.'),
        'Sign-in registration mismatch': ('REGISTRATION_MISMATCH', 'The returned registration differs from the account selected for this attempt. Start again with the intended account.'),
        'OpenAI identity token missing': ('IDENTITY_MISSING', 'OpenAI did not return the identity token required to verify this sign-in. Start a fresh sign-in.'),
        'OpenAI access token missing': ('ACCESS_TOKEN_MISSING', 'OpenAI did not return an access token. The session was not connected.'),
        'Identity nonce mismatch': ('IDENTITY_NONCE', 'The identity token does not match this sign-in attempt. Start again from Connections.'),
        'Signed-in identity differs from selected account': ('ACCOUNT_MISMATCH', 'The signed-in account differs from the saved account selected. Choose the intended account and sign in again.'),
        'Unexpected OpenAI identity endpoint': ('IDENTITY_ENDPOINT', 'OpenAI returned an unexpected identity endpoint. The session was not connected.'),
    }
    if isinstance(exc, Blocked) and str(exc) in blocked:
        code, message = blocked[str(exc)]
        return SignInFailure(code, message, stage)
    if isinstance(exc, RemoteError):
        if exc.code == 'invalid_grant':
            return SignInFailure('CODE_EXPIRED', 'OpenAI rejected the one-time authorization code. Start a fresh sign-in; the registered client ID has been retained. Do not reload the old callback.', stage)
        if exc.code == 'invalid_client':
            return SignInFailure('CLIENT_REJECTED', 'OpenAI rejected this client registration. Return to Connections and start a new registration.', stage)
        if exc.code in ('invalid_scope', 'access_denied'):
            return SignInFailure('PLAN_PERMISSION', 'OpenAI did not accept the requested ChatGPT plan permissions. Check the account and plan-usage authorization before retrying.', stage)
        status = f' (HTTP {exc.status})' if exc.status else ''
        return SignInFailure('OPENAI_UNAVAILABLE', 'The OpenAI '+stage.replace('_',' ')+' request failed'+status+'. Check connectivity and start a fresh sign-in.', stage)
    if isinstance(exc, jwt.ImmatureSignatureError):
        return SignInFailure('IDENTITY_CLOCK', 'The identity token is too far ahead of this computer clock. Sync Windows date/time, then start a fresh sign-in.', stage)
    if isinstance(exc, jwt.ExpiredSignatureError):
        return SignInFailure('IDENTITY_EXPIRED', 'The identity token is expired. Check Windows date/time and start a fresh sign-in.', stage)
    if isinstance(exc, jwt.InvalidSignatureError):
        return SignInFailure('IDENTITY_SIGNATURE', 'The identity signature could not be verified. No account credentials were activated.', stage)
    if isinstance(exc, (jwt.InvalidAudienceError, jwt.InvalidIssuerError)):
        return SignInFailure('IDENTITY_REGISTRATION', 'The identity token issuer or audience does not match this registration. Start again from Connections.', stage)
    if isinstance(exc, jwt.InvalidTokenError):
        return SignInFailure('IDENTITY_INVALID', 'OpenAI returned an identity token that could not be validated. No session was activated. Start a fresh sign-in.', stage)
    if isinstance(exc, jwt.PyJWKClientError):
        return SignInFailure('IDENTITY_KEYS', 'OpenAI identity signing keys could not be retrieved or matched. Check connectivity and start a fresh sign-in.', stage)
    if stage == 'credential_storage':
        return SignInFailure('CREDENTIAL_STORAGE', 'The verified session could not be saved in Windows encrypted storage. Check local folder access and retry.', stage)
    return SignInFailure('SIGNIN_FAILED', 'Sign-in could not finish during '+stage.replace('_',' ')+'. Return to Connections and start again.', stage)
