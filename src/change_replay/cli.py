DESCRIPTION='Transaction-aware change log replay with before-image validation'
import argparse, json, sqlite3, sys
from pathlib import Path
from .common import read, save
from .model import analyze
from . import audit, store

def run(source,history=False):
    data,digest=read(source)
    return {'schema_version':1,'source_sha256':digest,'result':analyze(data,history=history)}

def _top_parser():
    parser=argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument('--source',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--history',action='store_true',help='append SCD2 version history to the result')
    return parser

def _store_parser():
    parser=argparse.ArgumentParser(description=DESCRIPTION)
    commands=parser.add_subparsers(dest='command',required=True)
    init=commands.add_parser('init',help='initialise a database from an existing baseline')
    init.add_argument('--baseline',type=Path,required=True)
    init.add_argument('--db',type=Path,required=True)
    init_out=init.add_argument('--output',type=Path)
    ingest=commands.add_parser('ingest',help='durably receive one {transactions,watermark} batch')
    ingest.add_argument('--db',type=Path,required=True)
    ingest.add_argument('--batch',type=Path,required=True)
    ingest_out=ingest.add_argument('--output',type=Path)
    export=commands.add_parser('export',help='export tables, totals, progress and pending log')
    export.add_argument('--db',type=Path,required=True)
    export_out=export.add_argument('--output',type=Path)
    audit=commands.add_parser('audit',help='read-only compare a database against an NDJSON reference')
    audit.add_argument('--db',type=Path,required=True)
    audit.add_argument('--reference',type=Path,required=True)
    audit_out=audit.add_argument('--output',type=Path,required=True)
    for action in (init_out,ingest_out,export_out,audit_out):action.help='write the JSON summary to this path'
    return parser

def _emit(report,output):
    if output is not None:save(output,report)
    print(json.dumps(report,sort_keys=True,ensure_ascii=False,allow_nan=False))
    return report

def _store(args):
    if args.command=='init':
        if args.output is not None:
            if args.output.resolve()==args.db.resolve():raise ValueError('database and output must differ')
            if args.output.resolve()==args.baseline.resolve():raise ValueError('baseline and output must differ')
        document,_=read(args.baseline)
        return _emit(store.initialize(args.db,document),args.output)
    if args.command=='ingest':
        if args.output is not None:
            if args.output.resolve()==args.db.resolve():raise ValueError('database and output must differ')
            if args.output.resolve()==args.batch.resolve():raise ValueError('batch and output must differ')
        batch,_=read(args.batch)
        return _emit(store.ingest(args.db,batch),args.output)
    if args.command=='audit':
        if args.output.resolve()==args.db.resolve():raise ValueError('database and output must differ')
        if args.output.resolve()==args.reference.resolve():raise ValueError('reference and output must differ')
        return _emit(audit.run(args.db,args.reference),args.output)
    if args.output is not None and args.output.resolve()==args.db.resolve():
        raise ValueError('database and output must differ')
    return _emit(store.export(args.db),args.output)

def main(argv=None):
    argv=sys.argv[1:] if argv is None else list(argv)
    commands=('init','ingest','export','audit')
    try:
        if argv[:1] and argv[0] in commands:
            args=_store_parser().parse_args(argv)
            _store(args);return 0
        args=_top_parser().parse_args(argv)
        if args.source.resolve()==args.output.resolve():raise ValueError('input and output must differ')
        report=run(args.source,history=args.history);save(args.output,report)
    except (ValueError,TypeError,KeyError,OSError,OverflowError,RecursionError,sqlite3.Error) as error:
        print(str(error),file=sys.stderr);return 2
    print(json.dumps(report,sort_keys=True,ensure_ascii=False,allow_nan=False));return 0

if __name__=='__main__':raise SystemExit(main())
