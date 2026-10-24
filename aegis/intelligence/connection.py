"""ChatGPT-only connection for the paper workspace; no exchange client import."""
from pathlib import Path
from aegis.chatgpt import ChatGPT
from aegis.vault import Vault
from aegis.domain import Blocked


class PlanConnection:
    def __init__(self,service,port):
        self.service=service;self.vault=Vault(service.store.directory/'chatgpt-connection.dpapi')
        self.chat=ChatGPT(self.vault,port);self.error=None

    def status(self):
        s=self.chat.status()
        # Public UI never receives tokens, email, subjects or the saved profile.
        return {'connected':s['connected'],'plan_enabled':s['plan_enabled'],'models':s['models'],
                'selected_model':self.service.spot.analyst.model,'enabled':self.service.spot.analyst.economy.enabled,
                'error':self.error,'billing':'ChatGPT plan allowance; not free or covered by virtual funds'}

    def saved(self):
        """Only an explicit user action invokes this local migration. Never mutate the source vault."""
        self.disable()
        source=Path('C:/Users/HP/Music/Aegis-Crypto-Agent/binance-v3/data/credentials.dpapi')
        if not source.is_file():raise Blocked('No saved ChatGPT connection found; use Continue with ChatGPT')
        data=Vault(source).read();saved=data.get('chatgpt',{});active=saved.get('active');p=saved.get('profiles',{}).get(active)
        if not p or not p.get('access_token'):raise Blocked('Saved ChatGPT connection is unavailable; sign in here')
        allowed=('client_id','sub','email','name','access_token','refresh_token','id_token','scopes','expires_at')
        self.vault.update('chatgpt',{'active':active,'profiles':{active:{k:p[k] for k in allowed if k in p}}})
        if data.get('host_id'):self.vault.update('host_id',data['host_id'])
        return self.catalog()

    def catalog(self):
        try:
            models=self.chat.models(refresh=True);self.error=None
            selected=self.service.store.state('spot_selected_model')
            available={m['slug'] for m in models}
            sol=[m['slug'] for m in models if m['slug'].lower().startswith('gpt-6-sol')]
            # Preserve a prior exact selection. Auto-select only a unique Sol entry.
            if selected in available:self.select(selected,known=available)
            elif not selected and len(sol)==1:self.select(sol[0],known=available)
            elif selected:
                self.disable();self.error='The selected model is unavailable. Select an available GPT-6 Sol entry; no fallback was used.'
            return {'models':models}
        except Exception:
            self.error='ChatGPT model list unavailable; reconnect or refresh. No inference was attempted.'
            raise Blocked(self.error) from None

    def select(self,model,known=None):
        available=known if known is not None else {m['slug'] for m in self.chat.models()}
        if model not in available:raise Blocked('Select an exact model from this account catalog')
        if not self.chat.status()['plan_enabled']:raise Blocked('ChatGPT plan-use permission is required')
        old=self.service.spot.analyst
        if old.model!=model or not old.economy.enabled:
            self.service.pause()
            if getattr(self.service,'control',None):self.service.control._event('PAUSED','ChatGPT connected. Press Start to scan with GPT assessment.')
        self.service.spot.analyst.attach(self.chat,model)
        self.service.store.put_state('spot_selected_model',model);self.service.store.put_state('spot_model_authorized',True)
        return {'selected_model':model}

    def restore(self):
        # Local-only restore: do not fetch a catalog or invoke a model at boot.
        model=self.service.store.state('spot_selected_model')
        if self.service.store.state('spot_model_authorized') and model and self.chat.status()['plan_enabled']:
            self.service.spot.analyst.attach(self.chat,model)

    def disable(self):
        self.service.pause();self.service.spot.analyst.economy.enabled=False
        self.service.store.put_state('spot_model_authorized',False)
