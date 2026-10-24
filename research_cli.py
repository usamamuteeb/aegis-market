"""Download public history and screen the fixed strategy; never access a wallet."""
import argparse
from pathlib import Path
import json
from aegis.config import DEFAULTS
from aegis.research import run_screening

if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--output',type=Path,required=True)
    args=p.parse_args()
    report=run_screening({**DEFAULTS,'allocation':10},args.output,progress=lambda m:print(m,flush=True))
    print(json.dumps({'report':str(args.output),'live_eligible':False,
                      'metrics':{n:{c:b[c]['metrics'] for c in ('normal_costs','stressed_costs')} for n,b in report['blocks'].items()}},indent=2))
