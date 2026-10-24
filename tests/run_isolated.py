"""Run all fixtures with remote networking and production SQLite access denied."""
import socket
import sqlite3
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parent.parent));sys.path.insert(0,str(Path(__file__).resolve().parent))
connect=socket.socket.connect;resolve=socket.getaddrinfo;database=sqlite3.connect
def guarded_connect(sock,address):
    if isinstance(address,tuple) and address[0] not in ('127.0.0.1','::1','localhost'):raise AssertionError('Remote networking forbidden in fixture tests')
    return connect(sock,address)
def guarded_resolve(host,*args,**kwargs):
    if host not in ('127.0.0.1','::1','localhost',None):raise AssertionError('Remote DNS forbidden in fixture tests')
    return resolve(host,*args,**kwargs)
def guarded_database(path,*args,**kwargs):
    if 'music/aegis-crypto-agent' in str(path).replace('\\','/').lower():raise AssertionError('Production database access forbidden in tests')
    return database(path,*args,**kwargs)
with patch('socket.socket.connect',guarded_connect),patch('socket.getaddrinfo',guarded_resolve),patch('sqlite3.connect',guarded_database):
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.discover(str(Path(__file__).resolve().parent),pattern='test_*.py'))
raise SystemExit(not result.wasSuccessful())
