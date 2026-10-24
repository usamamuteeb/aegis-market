"""Historical screening is evidence, never a guarantee or an automatic live release."""
import hashlib
import json
from pathlib import Path
from aegis.domain import Blocked

def code_fingerprint():
    root=Path(__file__).resolve().parent
    names=('strategy.py','risk.py','execution.py','config.py','assessment.py','research.py','engine.py')
    return hashlib.sha256(b''.join((root/n).read_bytes() for n in names)).hexdigest()

def status(report_path):
    report=None
    try: report=json.loads(Path(report_path).read_text(encoding='utf-8'))
    except (OSError,ValueError): pass
    current=bool(isinstance(report,dict) and report.get('code_fingerprint')==code_fingerprint())
    if current:
        try:
            report = {**report, 'blocks': {name: {**block, 'normal_costs': {k:v for k,v in block['normal_costs'].items() if k not in ('closed_trades','equity_curve')}, 'stressed_costs': {k:v for k,v in block['stressed_costs'].items() if k not in ('closed_trades','equity_curve')}} for name,block in report['blocks'].items()}}
        except (KeyError,TypeError,AttributeError): current=False; report=None
    return {'live_eligible':False,'stage':'HISTORICAL_SCREENING' if current else 'UNVALIDATED',
            'reason':'Live entries are locked: profitable historical screening, independent forward evidence, GPT comparison and exchange recovery acceptance have not all been established.',
            'report_current':current,'report':report if current else None,
            'missing':['Positive cost-adjusted historical evidence','Independent forward evidence after costs','Same-period GPT versus strategy-only comparison',
                       'Exchange testnet entry/protection/outage acceptance','Review before any real-money release'],
            'claim':'No demonstrated ability to turn $10 into $100; future profit and a maximum win rate cannot be guaranteed.'}

def require_live_release(environment, report_path):
    if environment=='live' and not status(report_path)['live_eligible']:
        raise Blocked('Live entries locked: this strategy has not completed qualification. Your real wallet is not a research account.')
