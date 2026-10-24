import hashlib
import json
from decimal import Decimal, ROUND_DOWN

class Blocked(ValueError):
    pass

def D(value):
    try:
        v = Decimal(str(value))
    except Exception:
        raise Blocked('Invalid numeric value') from None
    if not v.is_finite():
        raise Blocked('Non-finite numeric value')
    return v

def floor_step(value, step):
    return (D(value) / D(step)).to_integral_value(rounding=ROUND_DOWN) * D(step)

def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()

def number(value):
    return format(D(value), 'f')
