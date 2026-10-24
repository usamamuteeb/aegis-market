import math
from aegis.domain import Blocked

FIELDS = ('decision', 'symbol', 'proposed_entry', 'stop_loss', 'take_profit', 'explanation', 'invalidation', 'expires_at')
SCHEMA = {'type': 'object', 'additionalProperties': False, 'required': list(FIELDS), 'properties': {
    'decision': {'type': 'string', 'enum': ['BUY', 'WAIT', 'EXIT']}, 'symbol': {'type': 'string'},
    **{k: {'type': ['number', 'null']} for k in ('proposed_entry', 'stop_loss', 'take_profit')},
    'explanation': {'type': 'string'}, 'invalidation': {'type': 'array', 'items': {'type': 'string'}},
    'expires_at': {'type': 'integer'}}}

def validate(p, context, now):
    if not isinstance(p, dict) or set(p) != set(FIELDS):
        raise Blocked('Malformed model response; WAIT')
    if p['decision'] not in ('BUY', 'WAIT', 'EXIT') or p['symbol'] != context['symbol']:
        raise Blocked('Unsupported model decision; WAIT')
    if isinstance(p['expires_at'], bool) or not isinstance(p['expires_at'], int) or not now < p['expires_at'] <= context['expires_at'] + 1:
        raise Blocked('Expired assessment; WAIT')
    if not isinstance(p['explanation'], str) or not 1 <= len(p['explanation']) <= 1500 or not isinstance(p['invalidation'], list) or not 1 <= len(p['invalidation']) <= 5 or any(not isinstance(s, str) or not 1 <= len(s) <= 300 for s in p['invalidation']):
        raise Blocked('Malformed assessment explanation; WAIT')
    if p['decision'] == 'BUY' and context['event'] != 'candidate':
        raise Blocked('A position event cannot add to a position')
    if p['decision'] == 'EXIT' and context['event'] == 'candidate':
        raise Blocked('No managed position to exit')
    for field in ('proposed_entry', 'stop_loss', 'take_profit'):
        value = p[field]
        if p['decision'] == 'BUY':
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or abs(value - float(context['entry' if field == 'proposed_entry' else field])) > float(context['tick']) * .01:
                raise Blocked('Model attempted to change fixed levels; WAIT')
        elif value is not None:
            raise Blocked('WAIT/EXIT cannot change protective levels')
    return p
