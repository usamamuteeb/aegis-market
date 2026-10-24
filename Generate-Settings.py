"""Generate private deployment settings outside Git. Never includes exchange or ChatGPT tokens."""
import secrets
from pathlib import Path
from cryptography.fernet import Fernet
root=Path(__file__).resolve().parent;target=root/'.env.production.local'
if target.exists():raise SystemExit('Settings already exist. Keep the same encryption key to retain stored connections.')
password='Aegis-'+secrets.token_hex(5)
values={'AEGIS_USERNAME':'aegis','AEGIS_PASSWORD':password,'AEGIS_SESSION_SECRET':secrets.token_urlsafe(48),
 'AEGIS_ENCRYPTION_KEY':Fernet.generate_key().decode(),'AEGIS_ALLOCATION_USDT':'10','LIVE_TRADING_ENABLED':'true'}
target.write_text('\n'.join(k+'='+v for k,v in values.items())+'\n',encoding='utf-8')
(root/'cloud-login.txt').write_text('Username: aegis\nPassword: '+password+'\n\nKeep .env.production.local private. Its values belong only in your Vercel project environment.\n',encoding='utf-8')
print('Private settings saved to .env.production.local and cloud-login.txt. These files are excluded from Git.')
