"""Single-owner login; no signup, email verification, or credentials in browser storage."""
import base64,hashlib,hmac,json,secrets,time
from http.cookies import SimpleCookie

class Login:
    def __init__(self,username,password,secret,secure=True):
        if not username or len(password)<12 or len(secret)<32:raise ValueError('Set AEGIS_USERNAME, a 12+ character AEGIS_PASSWORD, and 32+ character AEGIS_SESSION_SECRET')
        self.username=username;self.password=password;self.secret=secret.encode();self.secure=secure
    def credentials(self,username,password):
        return hmac.compare_digest(str(username),self.username) and hmac.compare_digest(str(password),self.password)
    def token(self):
        body=base64.urlsafe_b64encode(json.dumps({'exp':int(time.time())+43200,'nonce':secrets.token_hex(16)}).encode()).decode()
        return body+'.'+hmac.new(self.secret,body.encode(),hashlib.sha256).hexdigest()
    def valid(self,headers):
        try:
            cookie=SimpleCookie();cookie.load(headers.get('Cookie',''));token=cookie['aegis_owner'].value
            body,signature=token.split('.')
            if not hmac.compare_digest(signature,hmac.new(self.secret,body.encode(),hashlib.sha256).hexdigest()):return None
            if json.loads(base64.urlsafe_b64decode(body))['exp']<=time.time():return None
            return token
        except (KeyError,ValueError,TypeError):return None
    def cookie(self,token):return 'aegis_owner='+token+'; HttpOnly; SameSite=Strict; Path=/; Max-Age=43200'+('; Secure' if self.secure else '')
    def csrf(self,token):return hmac.new(self.secret,('csrf:'+token).encode(),hashlib.sha256).hexdigest()

