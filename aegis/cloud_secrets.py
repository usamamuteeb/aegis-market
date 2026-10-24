"""Encrypted rotating credentials stored durably; encryption key stays in hosting secrets."""
import json,threading
from cryptography.fernet import Fernet

class CloudVault:
    def __init__(self,store,key):self.store=store;self.lock=store.lock;self.cipher=Fernet(key.encode())
    def read(self):
        with self.lock:
            blob=self.store.state('cloud_encrypted_credentials')
            return json.loads(self.cipher.decrypt(blob.encode())) if blob else {}
    def update(self,key,value):
        with self.lock,self.store.db:
            if hasattr(self.store,'own_worker'):self.store.db.execute('SELECT pg_advisory_xact_lock(194512713)')
            data=self.read()
            if value is None:data.pop(key,None)
            else:data[key]=value
            self.store.put_state('cloud_encrypted_credentials',self.cipher.encrypt(json.dumps(data).encode()).decode())

