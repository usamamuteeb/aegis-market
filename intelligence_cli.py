"""Explicit isolated entry point. No production settings or credential loading."""
import argparse
import json
from pathlib import Path
from aegis.intelligence.store import ResearchStore
from aegis.intelligence.service import IntelligenceService
from aegis.intelligence.evaluation import evaluate_dataset,register_holdout

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workspace',required=True,type=Path,help='New empty isolated folder; never a production data folder')
    sub=parser.add_subparsers(dest='command',required=True)
    serve=sub.add_parser('serve');serve.add_argument('--port',type=int,default=8777)
    serve.add_argument('--open-browser',action='store_true',help='Open this isolated dashboard after the listener is ready')
    research=sub.add_parser('research');research.add_argument('--symbols',nargs='+',default=['BTCUSDT'])
    research.add_argument('--intervals',nargs='+',choices=['1m','5m'],default=['1m','5m']);research.add_argument('--days',type=int,default=14);research.add_argument('--max-pages',type=int,default=30)
    sub.add_parser('evaluate');sub.add_parser('report')
    hold=sub.add_parser('register-holdout');hold.add_argument('--start',type=float,required=True);hold.add_argument('--end',type=float,required=True)
    args=parser.parse_args()
    if args.command=='serve':
        from paper_server import run
        return run(args.workspace,args.port,open_browser=args.open_browser)
    store=ResearchStore(args.workspace);service=IntelligenceService(store)
    try:
        if args.command=='research':
            runs=service.research(args.symbols,args.intervals,args.days,max_pages=args.max_pages)
            output={'evaluations':[r['id'] for r in runs],'job':service.job,'live_eligible':False,'model_calls':0}
        elif args.command=='evaluate':
            runs=[];seen=set()
            for record in store.records('datasets',limit=100):
                d=record['body'];key=(d['symbol'],d['interval'])
                if key in seen or not d['coverage_complete']:continue
                seen.add(key);rows=service.ingestor.load(record['id'])
                for cost in ({},{'fee':.002,'spread_bps':20,'slippage_bps':20}):runs.append(evaluate_dataset(store,record['id'],rows,d,**cost)['id'])
            output={'evaluations':runs,'live_eligible':False,'model_calls':0}
        elif args.command=='register-holdout':output={'holdout_id':register_holdout(store,args.start,args.end),'live_eligible':False}
        else:output=service.snapshot()
        print(json.dumps(output,indent=2,allow_nan=False))
        if args.command=='research' and service.job.get('error'):raise SystemExit(2)
    finally:service.close();store.close()

if __name__=='__main__':main()
