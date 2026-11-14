import unittest, tempfile, subprocess, sys, json, copy
from pathlib import Path
from change_replay.model import analyze
ROOT=Path(__file__).resolve().parents[1]

def base_input():
    return {'baseline':{'lsn':10,'customers':[{'key':'C1','value':{'name':'East'}}],
                        'orders':[{'key':'O1','value':{'customer_id':'C1','amount_cents':1000}}]},
            'transactions':[],'through_lsn':20}

def tx(tid,lsn,ops):return {'id':tid,'commit_lsn':lsn,'operations':ops}
def op(table,key,before,after):return {'table':table,'key':key,'before':before,'after':after}
def o1(amount):return {'customer_id':'C1','amount_cents':amount}

def restore(history,lsn):
    state={'customers':{},'orders':{}}
    for v in history:
        if v['from_lsn']<=lsn and (v['to_lsn'] is None or v['to_lsn']>lsn):
            state[v['table']][v['key']]=v['value']
    return state

def run_cli(source,out,*extra):
    command=[sys.executable,'-m','change_replay.cli','--source',str(source),'--output',str(out),*extra]
    return subprocess.run(command,capture_output=True)

class HistoryCLITests(unittest.TestCase):
    def run_history(self,data,td,*extra):
        source=Path(td)/'input.json';out=Path(td)/'report.json'
        source.write_text(json.dumps(data))
        result=run_cli(source,out,'--history',*extra)
        self.assertEqual(result.returncode,0,result.stderr)
        return json.loads(out.read_text()),out

    def test_delete_recreate_across_transactions(self):
        data=base_input()
        data['transactions']=[
            tx('T1',11,[op('orders','O1',o1(1000),None),op('customers','C1',{'name':'East'},None)]),
            tx('T2',13,[op('customers','C1',None,{'name':'East'}),op('orders','O1',None,o1(1500))])]
        with tempfile.TemporaryDirectory() as td:
            report,_=self.run_history(data,td)
        history=report['result']['history']
        self.assertEqual(history,[
            {'table':'customers','key':'C1','value':{'name':'East'},'from_lsn':10,'to_lsn':11},
            {'table':'customers','key':'C1','value':{'name':'East'},'from_lsn':13,'to_lsn':None},
            {'table':'orders','key':'O1','value':o1(1000),'from_lsn':10,'to_lsn':11},
            {'table':'orders','key':'O1','value':o1(1500),'from_lsn':13,'to_lsn':None}])
        # No tombstone versions: inside the gap the key simply does not exist.
        self.assertEqual(restore(history,12),{'customers':{},'orders':{}})

    def test_same_transaction_net_zero_change_keeps_version(self):
        data=base_input()
        data['transactions']=[
            tx('T1',11,[op('orders','O1',o1(1000),o1(1300)),op('orders','O1',o1(1300),o1(900)),
                        op('orders','O1',o1(900),o1(1000))]),
            tx('T2',12,[op('orders','O1',o1(1000),None),op('customers','C1',{'name':'East'},None),
                        op('customers','C1',None,{'name':'East'}),op('orders','O1',None,o1(1000))]),
            tx('T3',13,[op('orders','O1',o1(1000),o1(1100))])]
        with tempfile.TemporaryDirectory() as td:
            report,_=self.run_history(data,td)
        # Net-zero transactions never split a version; only T3's committed change does.
        self.assertEqual(report['result']['history'],[
            {'table':'customers','key':'C1','value':{'name':'East'},'from_lsn':10,'to_lsn':None},
            {'table':'orders','key':'O1','value':o1(1000),'from_lsn':10,'to_lsn':13},
            {'table':'orders','key':'O1','value':o1(1100),'from_lsn':13,'to_lsn':None}])
        self.assertEqual(report['result']['state']['orders']['O1'],o1(1100))

    def test_order_written_before_customer_in_one_transaction(self):
        data=base_input()
        data['transactions']=[tx('T1',11,[op('orders','O2',None,{'customer_id':'C2','amount_cents':700}),
                                          op('customers','C2',None,{'name':'West'})])]
        with tempfile.TemporaryDirectory() as td:
            report,_=self.run_history(data,td)
        self.assertEqual(report['result']['customer_order_cents'],{'C1':1000,'C2':700})
        self.assertEqual(report['result']['history'],[
            {'table':'customers','key':'C1','value':{'name':'East'},'from_lsn':10,'to_lsn':None},
            {'table':'customers','key':'C2','value':{'name':'West'},'from_lsn':11,'to_lsn':None},
            {'table':'orders','key':'O1','value':o1(1000),'from_lsn':10,'to_lsn':None},
            {'table':'orders','key':'O2','value':{'customer_id':'C2','amount_cents':700},'from_lsn':11,'to_lsn':None}])

    def test_out_of_order_retransmission_replays_once(self):
        data=base_input()
        t1=tx('T1',11,[op('customers','C2',None,{'name':'West'})])
        t2=tx('T2',12,[op('orders','O2',None,{'customer_id':'C2','amount_cents':300})])
        data['transactions']=[t2,copy.deepcopy(t1),t1]
        ordered=copy.deepcopy(data);ordered['transactions']=[t1,copy.deepcopy(t1),t2]
        with tempfile.TemporaryDirectory() as td:
            report,out=self.run_history(data,td)
            self.assertEqual(report['result'],analyze(ordered,history=True))
            self.assertEqual(report['result']['duplicate_transactions'],1)
            first=out.read_bytes()
            report2,_=self.run_history(data,td)
            self.assertEqual(first,out.read_bytes());self.assertEqual(report2,report)

    def test_bad_future_transaction_fails_and_keeps_prior_report(self):
        data=base_input();data['through_lsn']=10
        with tempfile.TemporaryDirectory() as td:
            report,out=self.run_history(data,td);prior=out.read_bytes()
            for bad_op in [op('ledger','X',None,{'name':'N'}),
                           op('orders','O9',None,{'customer_id':'C1','amount_cents':-5})]:
                bad=copy.deepcopy(data);bad['transactions']=[tx('T9',99,[bad_op])]
                (Path(td)/'bad.json').write_text(json.dumps(bad))
                result=run_cli(Path(td)/'bad.json',out,'--history')
                self.assertEqual(result.returncode,2);self.assertEqual(out.read_bytes(),prior)
            # A well-shaped future transaction is not state-checked and adds no versions.
            ok=copy.deepcopy(data)
            ok['transactions']=[tx('T9',99,[op('orders','O1',o1(42),None)])]
            report2,_=self.run_history(ok,td)
            self.assertEqual(report2['result']['future_transactions'],['T9'])
            self.assertEqual(report2['result']['history'],report['result']['history'])

class HistoryPropertyTests(unittest.TestCase):
    def test_fixture_history_matches_known_versions(self):
        data=json.loads((ROOT/'fixtures/input.json').read_text())
        r=analyze(data,history=True)
        self.assertEqual(r['history'],[
            {'table':'customers','key':'C1','value':{'name':'East'},'from_lsn':10,'to_lsn':12},
            {'table':'customers','key':'C2','value':{'name':'West'},'from_lsn':11,'to_lsn':None},
            {'table':'orders','key':'O1','value':o1(1000),'from_lsn':10,'to_lsn':11},
            {'table':'orders','key':'O1','value':o1(1200),'from_lsn':11,'to_lsn':12},
            {'table':'orders','key':'O2','value':{'customer_id':'C2','amount_cents':500},'from_lsn':11,'to_lsn':None}])
        self.assertNotIn('history',analyze(data))

    def test_restore_matches_replay_at_every_lsn_including_holes(self):
        data=base_input()
        data['transactions']=[
            tx('T1',11,[op('orders','O1',o1(1000),o1(1200)),op('customers','C2',None,{'name':'West'}),
                        op('orders','O2',None,{'customer_id':'C2','amount_cents':500})]),
            tx('T2',12,[op('orders','O1',o1(1200),None),op('customers','C1',{'name':'East'},None)]),
            tx('T3',15,[op('orders','O2',{'customer_id':'C2','amount_cents':500},
                          {'customer_id':'C2','amount_cents':900})])]
        data['through_lsn']=17
        history=analyze(data,history=True)['history']
        # Versions still valid at the cutoff stay open; holes restore to the last committed state.
        self.assertTrue(all(v['to_lsn'] is None or v['to_lsn']<=17 for v in history))
        self.assertEqual({(v['table'],v['key']) for v in history if v['to_lsn'] is None},
                         {('customers','C2'),('orders','O2')})
        for lsn in range(10,18):
            point=copy.deepcopy(data);point['through_lsn']=lsn
            self.assertEqual(restore(history,lsn),analyze(point)['state'],lsn)

    def test_history_is_additive_to_the_default_contract(self):
        source=ROOT/'fixtures/input.json'
        with tempfile.TemporaryDirectory() as td:
            plain=Path(td)/'plain.json';full=Path(td)/'full.json'
            self.assertEqual(run_cli(source,plain).returncode,0)
            self.assertEqual(run_cli(source,full,'--history').returncode,0)
            default=json.loads(plain.read_text());extended=json.loads(full.read_text())
        self.assertNotIn('history',default['result'])
        history=extended['result'].pop('history')
        self.assertEqual(extended,default)
        self.assertEqual(history,analyze(json.loads(source.read_text()),history=True)['history'])

if __name__=='__main__':unittest.main()
