"""Connection failures pause entries; recovery never automatically rearms them."""
class ConnectionGuard:
    def __init__(self):
        self.status='CONNECTING'; self.failures=0; self.good_reads=0
        self.last_healthy=None; self.next_retry_at=None; self.reason=None

    def observe(self, healthy, now, reason=None):
        was_healthy=self.status=='ONLINE'
        if not healthy:
            self.failures+=1; self.good_reads=0; self.status='UNAVAILABLE'
            self.reason=reason or 'Market or account data is unavailable'
            self.next_retry_at=now+min(30,3*2**min(self.failures-1,4))
            return was_healthy or self.failures==1
        self.good_reads+=1; self.last_healthy=now; self.next_retry_at=None
        self.reason=None
        self.status='ONLINE' if self.good_reads>=2 else 'RECOVERING'
        if self.status=='ONLINE': self.failures=0
        return False

    def snapshot(self):
        return {'status':self.status,'failures':self.failures,'last_healthy':self.last_healthy,
                'next_retry_at':self.next_retry_at,'reason':self.reason,
                'resume_policy':'Entries remain paused after a failure; recovery does not rearm trading'}
