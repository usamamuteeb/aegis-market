"""Presentation truth derived from authoritative OCO list AND child states."""
ACTIVE={'NEW','PARTIALLY_FILLED'}
def protection_state(planned=False,intent=None,order_list=None,children=None):
    if not intent:return 'LOCALLY_PLANNED' if planned else 'ABSENT'
    if intent.get('status')=='REJECTED':return 'REJECTED'
    if intent.get('status') in ('SENDING','UNKNOWN'):return 'UNKNOWN' if intent['status']=='UNKNOWN' else 'SUBMISSION_PENDING'
    if not order_list or children is None:return 'UNKNOWN'
    expected={o.get('orderId') for o in order_list.get('orders',[])}
    actual={o.get('orderId') for o in children}
    if not expected or expected!=actual or any(o.get('symbol')!=order_list.get('symbol') for o in children):return 'UNKNOWN'
    if order_list.get('listOrderStatus')=='ALL_DONE' and all(o.get('status') in ('FILLED','CANCELED','EXPIRED','EXPIRED_IN_MATCH','REJECTED') for o in children):return 'TERMINAL'
    stop=[o for o in children if o.get('type') in ('STOP_LOSS','STOP_LOSS_LIMIT')]
    targets=[o for o in children if o.get('type') in ('LIMIT_MAKER','TAKE_PROFIT','TAKE_PROFIT_LIMIT')]
    if order_list.get('listOrderStatus')=='EXECUTING' and order_list.get('listStatusType')=='EXEC_STARTED' and len(stop)==len(targets)==1 and all(o.get('side')=='SELL' and o.get('status') in ACTIVE for o in children):return 'CONFIRMED_EXCHANGE_HELD'
    return 'UNKNOWN'
