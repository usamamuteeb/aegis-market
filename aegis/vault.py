"""Windows DPAPI encrypts credentials for this Windows user; never browser storage."""
import ctypes
from ctypes import wintypes
import json
import os
import threading
from pathlib import Path

class Blob(ctypes.Structure):
    _fields_ = [('cbData', wintypes.DWORD), ('pbData', ctypes.POINTER(ctypes.c_ubyte))]

def protect(data, decrypt=False):
    if os.name != 'nt':
        raise RuntimeError('This build requires Windows DPAPI for credential storage')
    buf = ctypes.create_string_buffer(data)
    source = Blob(len(data), ctypes.cast(buf, ctypes.POINTER(ctypes.c_ubyte)))
    target = Blob()
    crypt = ctypes.WinDLL('crypt32', use_last_error=True)
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    fn = crypt.CryptUnprotectData if decrypt else crypt.CryptProtectData
    fn.restype = wintypes.BOOL
    if not fn(ctypes.byref(source), None, None, None, None, 1, ctypes.byref(target)):
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        return ctypes.string_at(target.pbData, target.cbData)
    finally:
        kernel.LocalFree.argtypes = [ctypes.c_void_p]
        kernel.LocalFree(ctypes.cast(target.pbData, ctypes.c_void_p))

class Vault:
    def __init__(self, path):
        self.path = Path(path)
        self.lock = threading.RLock()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def read(self):
        with self.lock:
            return json.loads(protect(self.path.read_bytes(), True)) if self.path.exists() else {}

    def update(self, key, value):
        with self.lock:
            data = self.read()
            if value is None:
                data.pop(key, None)
            else:
                data[key] = value
            temp = self.path.with_suffix('.tmp')
            temp.write_bytes(protect(json.dumps(data).encode()))
            temp.replace(self.path)
