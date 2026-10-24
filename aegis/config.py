from pathlib import Path
from dataclasses import dataclass, asdict
from aegis.domain import Blocked, D

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / 'data'
PORT = 8765
SYMBOLS = ('BTCUSDT', 'ETHUSDT', 'SOLUSDT')

@dataclass(frozen=True)
class StrategySettings:
    atr_stop_multiple: float = 1.5
    min_stop_pct: float = .5
    max_stop_pct: float = 3
    reward_risk: float = 2
    min_volume_ratio: float = .8
    pullback_atr: float = .6
    proposal_ttl: int = 90

STRATEGY = StrategySettings()
DEFAULTS = {'allocation': None, 'max_position_pct': 80, 'risk_per_trade': .10,
            'daily_loss_limit': .50, 'max_trades_per_day': 5, 'max_consecutive_losses': 3,
            'max_drawdown_pct': 10, 'max_spread_bps': 25, 'slippage_bps': 10,
            'symbols': list(SYMBOLS), 'model': None}

def validate_settings(values):
    if set(values) != set(DEFAULTS):
        raise Blocked('Unexpected or missing settings')
    ranges = {'allocation': (.01, 1000000), 'max_position_pct': (1, 100),
              'risk_per_trade': (.001, 10000), 'daily_loss_limit': (.01, 100000),
              'max_trades_per_day': (1, 100), 'max_consecutive_losses': (1, 20),
              'max_drawdown_pct': (1, 50), 'max_spread_bps': (1, 100), 'slippage_bps': (0, 100)}
    for key, (lo, hi) in ranges.items():
        if key == 'allocation' and values[key] is None:
            continue
        if isinstance(values[key], bool) or not D(lo) <= D(values[key]) <= D(hi):
            raise Blocked('Invalid setting: ' + key)
    for k in ('max_trades_per_day', 'max_consecutive_losses'):
        if D(values[k]) != int(D(values[k])):
            raise Blocked(k + ' must be an integer')
    if not isinstance(values['symbols'], list) or not values['symbols'] or len(set(values['symbols'])) != len(values['symbols']) or any(s not in SYMBOLS for s in values['symbols']):
        raise Blocked('Choose supported Spot symbols')
    if values['model'] is not None and (not isinstance(values['model'], str) or len(values['model']) > 150):
        raise Blocked('Invalid model selection')
    if values['allocation'] and D(values['risk_per_trade']) > D(values['daily_loss_limit']):
        raise Blocked('Per-trade loss must not exceed daily loss limit')
    clean = dict(values)
    for key in ranges:
        if clean[key] is not None:
            clean[key] = int(D(clean[key])) if key in ('max_trades_per_day', 'max_consecutive_losses') else float(D(clean[key]))
    return clean
