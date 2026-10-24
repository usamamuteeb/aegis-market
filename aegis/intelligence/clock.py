"""Bounded exchange-clock estimate. Never changes the operating-system clock."""
import threading
import time


class ExchangeClock:
    def __init__(self):
        self.lock=threading.RLock();self.sample=None

    def update(self,server_seconds,wall_start,wall_end,mono_start,mono_end):
        rtt=mono_end-mono_start
        if not 0<=rtt<=2 or abs((wall_end-wall_start)-rtt)>.25:
            raise ValueError('Exchange time sample too uncertain or PC clock changed')
        offset=server_seconds-(wall_start+wall_end)/2
        if abs(offset)>30:raise ValueError('PC clock differs from Binance by over 30 seconds')
        with self.lock:self.sample={'server_at_end':server_seconds+rtt/2,'mono':mono_end,'wall':wall_end,
                                    'offset_seconds':offset,'uncertainty_seconds':rtt/2}

    def now(self):
        with self.lock:s=self.sample
        return s['server_at_end']+time.monotonic()-s['mono'] if s else time.time()

    def status(self):
        with self.lock:s=self.sample
        if not s:return {'ready':False,'reason':'Waiting for exchange clock calibration','offset_seconds':None,'uncertainty_seconds':None}
        age=time.monotonic()-s['mono'];jump=abs(time.time()-s['wall']-age)
        reason='Clock calibration expired' if age>240 else 'PC clock changed; recalibration required' if jump>1 else None
        return {'ready':reason is None,'reason':reason,'sample_age_seconds':age,
                'offset_seconds':s['offset_seconds'],'uncertainty_seconds':s['uncertainty_seconds']}
