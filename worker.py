"""Vercel daemon entry point. PostgreSQL lock fences overlapping deployments."""
import os,signal,threading,time
from aegis.cloud_runtime import open_store,build_runtime,view

def main():
    store=open_store()
    if hasattr(store,'own_worker'):
        while not store.own_worker():time.sleep(5)
    service,supervisor=build_runtime(store,os.environ['AEGIS_ENCRYPTION_KEY'],
        allow_live=os.environ.get('LIVE_TRADING_ENABLED','false').lower()=='true',allocation=float(os.environ.get('AEGIS_ALLOCATION_USDT','10')))
    shutdown=threading.Event()
    progress=[time.monotonic()]
    def watchdog():
        while not shutdown.wait(5):
            if time.monotonic()-progress[0]>120:
                print('Worker loop stalled; restarting for durable order reconciliation',flush=True);os._exit(1)
    threading.Thread(target=watchdog,daemon=True,name='worker-watchdog').start()
    for sig in (signal.SIGTERM,signal.SIGINT):signal.signal(sig,lambda *_:shutdown.set())
    last_snapshot=0
    try:
        while not shutdown.wait(.25):
            # Database failure is fatal: lose the worker and its lock, then let
            # Vercel restart. Never keep trading without durable order intents.
            supervisor.tick();service.dispatch_pending()
            progress[0]=time.monotonic()
            if time.monotonic()-last_snapshot>=2:
                service.store.put_state('cloud_snapshot',view(service,supervisor));last_snapshot=time.monotonic()
    finally:
        service.pause();service.stop.set();service.control.close()
        if getattr(service,'stream',None):service.stream.close()
        service.close();store.close()

if __name__=='__main__':main()
