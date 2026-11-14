import unittest,json,copy
from pathlib import Path
from change_replay.model import analyze
from change_replay.transaction import apply
ROOT=Path(__file__).resolve().parents[1]
class ReplayTests(unittest.TestCase):
    def setUp(self):self.d=json.loads((ROOT/'fixtures/input.json').read_text())
    def test_known_snapshot_and_exact_duplicate(self):
        r=analyze(self.d);self.assertEqual(r['customer_order_cents'],{'C2':500});self.assertEqual(r['duplicate_transactions'],1)
        self.assertEqual(r['last_applied_lsn'],12);self.assertEqual(r['future_transactions'],['T3'])
    def test_cutoff_retains_whole_transactions(self):
        self.d['through_lsn']=11;r=analyze(self.d);self.assertEqual(r['customer_order_cents'],{'C1':1200,'C2':500})
    def test_future_never_changes_current_state(self):
        r=analyze(self.d);self.d['transactions'][-1]['operations'][0]['after']['customer_id']='UNKNOWN'
        self.assertEqual(r['state'],analyze(self.d)['state'])
        self.d['through_lsn']=15
        with self.assertRaises(ValueError):analyze(self.d)
    def test_bad_before_image_and_conflicting_duplicate(self):
        self.d['transactions'][0]['operations'][0]['before']['amount_cents']=999
        with self.assertRaisesRegex(ValueError,'before'):analyze(self.d)
        self.setUp();self.d['transactions'][2]=copy.deepcopy(self.d['transactions'][1]);self.d['transactions'][2]['operations']=[]
        with self.assertRaisesRegex(ValueError,'conflicting'):analyze(self.d)
    def test_deferred_reference_and_failed_transaction_isolation(self):
        state={'customers':{},'orders':{}};original=copy.deepcopy(state)
        bad=[{'table':'orders','key':'O','before':None,'after':{'customer_id':'MISSING','amount_cents':2}}]
        with self.assertRaises(ValueError):apply(state,bad)
        self.assertEqual(state,original)
        r=analyze(self.d);self.assertIn('C2',r['state']['customers'])
    def test_transaction_order_independence_and_empty_log(self):
        r=analyze(self.d);self.d['transactions'].reverse();self.assertEqual(r,analyze(self.d))
        self.d['transactions']=[];r=analyze(self.d);self.assertEqual(r['last_applied_lsn'],10)
    def test_negative_amount_and_unknown_table_rejected(self):
        self.d['baseline']['orders'][0]['value']['amount_cents']=-1
        with self.assertRaises(ValueError):analyze(self.d)
        self.setUp();self.d['transactions'][0]['operations'][0]['table']='else'
        with self.assertRaises(ValueError):analyze(self.d)
if __name__=='__main__':unittest.main()
