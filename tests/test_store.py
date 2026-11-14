import unittest, json, os, sqlite3, subprocess, sys, tempfile, textwrap
from pathlib import Path
from change_replay import store
from change_replay.model import analyze

ROOT=Path(__file__).resolve().parents[1]


def baseline():
    return {'lsn':10,'customers':[{'key':'C1','value':{'name':'East'}}],
            'orders':[{'key':'O1','value':{'customer_id':'C1','amount_cents':1000}}]}


def tx(tid,lsn,ops):return {'id':tid,'commit_lsn':lsn,'operations':ops}
def op(table,key,before,after):return {'table':table,'key':key,'before':before,'after':after}
def cust(name):return {'name':name}
def o1(amount):return {'customer_id':'C1','amount_cents':amount}


def setup(db,*transactions,watermark=10):
    store.initialize(db,{'baseline':baseline()})
    if transactions:
        store.ingest(db,{'transactions':list(transactions),'watermark':watermark})


class InitTests(unittest.TestCase):
    def test_init_reads_the_existing_baseline_document(self):
        fixture=json.loads((ROOT/'fixtures/input.json').read_text())
        with tempfile.TemporaryDirectory() as td:
            db=Path(td)/'state.db'
            summary=store.initialize(db,fixture)
            self.assertEqual((summary['baseline_lsn'],summary['watermark'],
                              summary['last_applied_lsn']),(10,10,10))
            # The legacy CLI input carries transactions; init must ignore them.
            exported=store.export(db)
            self.assertEqual(exported['state'],{'customers':{'C1':{'name':'East'}},
                                                'orders':{'O1':o1(1000)}})
            self.assertEqual(exported['pending_transactions'],[])

    def test_init_is_create_only_and_leaves_no_database_on_failure(self):
        with tempfile.TemporaryDirectory() as td:
            db=Path(td)/'state.db'
            store.initialize(db,{'baseline':baseline()})
            prior=db.read_bytes()
            with self.assertRaisesRegex(ValueError,'already exists'):
                store.initialize(db,{'baseline':baseline()})
            self.assertEqual(db.read_bytes(),prior)
            bad={'baseline':{'lsn':10,'customers':[],
                             'orders':[{'key':'O1','value':{'customer_id':'X','amount_cents':1}}]}}
            with self.assertRaisesRegex(ValueError,'orphan'):
                store.initialize(Path(td)/'new.db',bad)
            self.assertEqual(sorted(p.name for p in Path(td).iterdir()),['state.db'])

    def test_uninitialized_or_foreign_database_is_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            missing=Path(td)/'missing.db'
            with self.assertRaisesRegex(ValueError,'not initialized'):
                store.export(missing)
            self.assertFalse(missing.exists())
            foreign=Path(td)/'foreign.db'
            connection=sqlite3.connect(foreign);connection.execute('CREATE TABLE other(x)');connection.commit();connection.close()
            with self.assertRaisesRegex(ValueError,'not initialized'):
                store.ingest(foreign,{'transactions':[],'watermark':10})

    def test_baseline_failures_leave_no_database(self):
        with tempfile.TemporaryDirectory() as td:
            cases=[{'lsn':10,'customers':[{'key':'C1','value':{'name':'East'}},
                                          {'key':'C1','value':{'name':'West'}}],'orders':[]},
                   {'lsn':-1,'customers':[],'orders':[]},
                   {'lsn':10,'customers':[{'key':' ','value':{'name':'East'}}],'orders':[]}]
            for case in cases:
                db=Path(td)/'state.db'
                with self.assertRaises(ValueError):store.initialize(db,{'baseline':case})
                self.assertFalse(db.exists())


class IngestTests(unittest.TestCase):
    def test_watermark_is_a_receive_frontier_not_applied_progress(self):
        with tempfile.TemporaryDirectory() as td:
            db=Path(td)/'state.db';setup(db)
            # LSN 11 is applied; the hole at 12 is permanent because the
            # watermark already promised no first arrival at or below it.
            store.ingest(db,{'transactions':[tx('T11',11,[op('customers','C2',None,cust('West'))]),
                                             tx('T13',13,[op('orders','O2',None,
                                                             {'customer_id':'C2','amount_cents':500})])],
                             'watermark':13})
            exported=store.export(db)
            self.assertEqual((exported['watermark'],exported['last_applied_lsn']),(13,13))
            self.assertEqual(store.ingest(db,{'transactions':[],'watermark':13})['duplicate_transactions'],0)
            exported=store.export(db)
            self.assertEqual((exported['watermark'],exported['last_applied_lsn']),(13,13))
            with self.assertRaisesRegex(ValueError,'below the existing watermark'):
                store.ingest(db,{'transactions':[tx('T12',12,[op('customers','C1',{'name':'East'},None)])],
                                 'watermark':13})
            self.assertEqual(store.export(db),exported)

    def test_empty_transaction_id_is_rejected_like_the_legacy_contract(self):
        with tempfile.TemporaryDirectory() as td:
            db=Path(td)/'state.db';setup(db)
            prior=store.export(db)
            with self.assertRaises(ValueError):
                store.ingest(db,{'transactions':[tx(' ',11,[])],'watermark':11})
            self.assertEqual(store.export(db),prior)

    def test_out_of_order_batches_and_retransmitted_content_are_idempotent(self):
        t1=tx('T1',11,[op('customers','C2',None,cust('West'))])
        t2=tx('T2',12,[op('orders','O2',None,{'customer_id':'C2','amount_cents':300})])
        t3=tx('T3',14,[op('orders','O2',{'customer_id':'C2','amount_cents':300},
                          {'customer_id':'C2','amount_cents':900})])
        with tempfile.TemporaryDirectory() as td:
            db=Path(td)/'state.db';setup(db)
            first=store.ingest(db,{'transactions':[t3,t1],'watermark':11})
            self.assertEqual([a['id'] for a in first['applied_transactions']],['T1'])
            self.assertEqual(store.export(db)['pending_transactions'],
                             [{'id':'T3','commit_lsn':14,'operations':1}])
            second=store.ingest(db,{'transactions':[t2,t1],'watermark':14})
            self.assertEqual(second['duplicate_transactions'],1)
            self.assertEqual([a['id'] for a in second['applied_transactions']],['T2','T3'])
            state=store.export(db)
            expected={'customers':{'C1':{'name':'East'},'C2':{'name':'West'}},
                      'orders':{'O1':o1(1000),'O2':{'customer_id':'C2','amount_cents':900}}}
            self.assertEqual(state['state'],expected)
            previous=Path(td,'snapshot.json');previous.write_text(json.dumps(state,sort_keys=True))
            replay=store.ingest(db,{'transactions':[t1,t2,t3],'watermark':14})
            self.assertEqual(replay['duplicate_transactions'],3)
            self.assertEqual(replay['applied_transactions'],[])
            self.assertEqual(json.dumps(store.export(db),sort_keys=True),previous.read_text())

    def test_identity_conflicts_and_stale_unknown_transactions_are_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            db=Path(td)/'state.db'
            setup(db,tx('T1',11,[op('customers','C2',None,cust('West'))]),watermark=12)
            prior=store.export(db)
            original=tx('T1',11,[op('customers','C2',None,cust('West'))])
            conflicted=tx('T1',11,[op('customers','C3',None,cust('North'))])
            moved_lsn=tx('T1',13,[op('customers','C3',None,cust('North'))])
            for bad in (conflicted,moved_lsn):
                with self.assertRaisesRegex(ValueError,'conflicting'):
                    store.ingest(db,{'transactions':[bad],'watermark':13})
            with self.assertRaisesRegex(ValueError,'conflicting'):
                store.ingest(db,{'transactions':[tx('T9',11,[op('customers','C3',None,cust('North'))])],
                                 'watermark':13})
            with self.assertRaisesRegex(ValueError,'below the existing watermark'):
                store.ingest(db,{'transactions':[tx('T9',12,[op('customers','C3',None,cust('North'))])],
                                 'watermark':13})
            self.assertEqual(store.export(db),prior)
            # Retransmission of the exact stored content is still a duplicate, in any batch position.
            result=store.ingest(db,{'transactions':[original,original],'watermark':12})
            self.assertEqual(result['duplicate_transactions'],2)

    def test_future_transactions_are_shape_checked_then_applied_later(self):
        future=tx('T9',20,[op('orders','O9',None,{'customer_id':'C9','amount_cents':50})])
        with tempfile.TemporaryDirectory() as td:
            db=Path(td)/'state.db';setup(db,future)
            self.assertEqual(store.export(db)['pending_transactions'],
                             [{'id':'T9','commit_lsn':20,'operations':1}])
            # The future transaction is not state-checked when received.
            self.assertNotIn('O9',store.export(db)['state']['orders'])
            with self.assertRaisesRegex(ValueError,'orphan'):
                store.ingest(db,{'transactions':[],'watermark':20})
            self.assertEqual(store.export(db)['watermark'],10)
            # T9 cannot be applied until its customer arrives in an earlier
            # transaction; that arrival is itself future when first received.
            add_customer=tx('T10',19,[op('customers','C9',None,cust('South'))])
            store.ingest(db,{'transactions':[add_customer],'watermark':19})
            self.assertEqual(store.export(db)['pending_transactions'],
                             [{'id':'T9','commit_lsn':20,'operations':1}])
            self.assertEqual(store.export(db)['last_applied_lsn'],19)
            store.ingest(db,{'transactions':[],'watermark':20})
            exported=store.export(db)
            self.assertEqual(exported['pending_transactions'],[])
            self.assertEqual(exported['state']['orders']['O9'],
                             {'customer_id':'C9','amount_cents':50})
            self.assertEqual(exported['customer_order_cents'],{'C1':1000,'C9':50})

            malformed=[op('ledger','X',None,{'name':'N'}),
                       op('orders','O9',None,{'customer_id':'C1','amount_cents':-1}),
                       op('orders','O9',None,None),
                       {'table':'orders','key':'O9','before':None},
                       {'table':'orders','key':' ','before':None,'after':o1(1)}]
            for bad_op in malformed:
                with self.assertRaises(ValueError):
                    store.ingest(db,{'transactions':[tx('T30',30,[bad_op])],'watermark':21})
            with self.assertRaisesRegex(ValueError,'must not decrease'):
                store.ingest(db,{'transactions':[],'watermark':19})
            self.assertEqual(store.export(db)['watermark'],20)

    def test_before_image_and_committed_foreign_key_errors_leave_the_batch_untouched(self):
        with tempfile.TemporaryDirectory() as td:
            db=Path(td)/'state.db';setup(db)
            prior=store.export(db)
            cases=[
                [tx('T1',11,[op('orders','O1',o1(5),None)])],                          # wrong before image
                [tx('T1',11,[op('customers','C1',{'name':'West'},None)])],             # wrong before image
                [tx('T1',11,[op('orders','O1',o1(1000),o1(1100))]),
                 tx('T2',12,[op('orders','O1',o1(999),None)])],                        # second transaction fails
                [tx('T1',11,[op('orders','O2',None,{'customer_id':'C9','amount_cents':1})])],  # orphan at commit
                [tx('T1',11,[op('customers','C1',{'name':'East'},cust('West'))]),
                 tx('T2',12,[op('customers','C1',{'name':'East'},None)])]]              # stale cross-batch before
            for transactions in cases:
                with self.assertRaises(ValueError):
                    store.ingest(db,{'transactions':transactions,'watermark':12})
                self.assertEqual(store.export(db),prior)

    def test_conflicts_are_detected_against_transactions_still_pending(self):
        with tempfile.TemporaryDirectory() as td:
            db=Path(td)/'state.db'
            pending=tx('T7',15,[op('customers','C2',None,cust('West'))])
            setup(db,pending,watermark=11)
            with self.assertRaisesRegex(ValueError,'conflicting'):
                store.ingest(db,{'transactions':[tx('T7',15,[op('customers','C2',None,cust('North'))])],
                                 'watermark':11})
            with self.assertRaisesRegex(ValueError,'conflicting'):
                store.ingest(db,{'transactions':[tx('T8',15,[op('customers','C2',None,cust('West'))])],
                                 'watermark':11})
            self.assertEqual(store.export(db)['pending_transactions'],
                             [{'id':'T7','commit_lsn':15,'operations':1}])

    def test_pending_transaction_failing_on_promotion_leaves_batch_unchanged(self):
        with tempfile.TemporaryDirectory() as td:
            db=Path(td)/'state.db'
            store.initialize(db,{'baseline':baseline()})
            first=tx('T20',19,[op('customers','C1',{'name':'East'},cust('West'))])
            second=tx('T21',20,[op('customers','C1',{'name':'East'},cust('North'))])
            store.ingest(db,{'transactions':[first,second],'watermark':10})
            self.assertEqual([p['id'] for p in store.export(db)['pending_transactions']],
                             ['T20','T21'])
            store.ingest(db,{'transactions':[],'watermark':19})
            after_first=store.export(db)
            self.assertEqual(after_first['state']['customers']['C1'],{'name':'West'})
            self.assertEqual([p['id'] for p in after_first['pending_transactions']],['T21'])
            # Promotion at LSN 20 hits a stale before image; the whole batch rolls
            # back and T21 stays pending at the old watermark.
            with self.assertRaisesRegex(ValueError,'before'):
                store.ingest(db,{'transactions':[],'watermark':20})
            self.assertEqual(store.export(db),after_first)
            self.assertEqual(store.export(db)['watermark'],19)

    def test_deferred_replay_uses_the_contract_order(self):
        data=json.loads((ROOT/'fixtures/input.json').read_text())
        with tempfile.TemporaryDirectory() as td:
            db=Path(td)/'state.db'
            store.initialize(db,{'baseline':data['baseline']})
            store.ingest(db,{'transactions':list(reversed(data['transactions'])),'watermark':12})
            exported=store.export(db)
            legacy=analyze(data)
            self.assertEqual(exported['state'],legacy['state'])
            self.assertEqual(exported['customer_order_cents'],legacy['customer_order_cents'])
            self.assertEqual(exported['last_applied_lsn'],legacy['last_applied_lsn'])
            self.assertEqual(exported['pending_transactions'],
                             [{'id':'T3','commit_lsn':15,'operations':1}])


class ExportTests(unittest.TestCase):
    def test_export_reports_tables_totals_watermark_and_pending(self):
        with tempfile.TemporaryDirectory() as td:
            db=Path(td)/'state.db'
            setup(db,tx('T1',11,[op('customers','C2',None,cust('West')),
                                 op('orders','O2',None,{'customer_id':'C2','amount_cents':500})]),
                  tx('T4',19,[op('customers','C2',{'name':'West'},cust('North'))]),watermark=15)
            exported=store.export(db)
            self.assertEqual(set(exported),{'schema_version','watermark','last_applied_lsn',
                                            'state','customer_order_cents','pending_transactions'})
            self.assertEqual(exported['watermark'],15)
            self.assertEqual(exported['last_applied_lsn'],11)
            self.assertEqual(exported['state'],{'customers':{'C1':{'name':'East'},'C2':{'name':'West'}},
                                                'orders':{'O1':o1(1000),
                                                          'O2':{'customer_id':'C2','amount_cents':500}}})
            self.assertEqual(exported['customer_order_cents'],{'C1':1000,'C2':500})
            self.assertEqual(exported['pending_transactions'],
                             [{'id':'T4','commit_lsn':19,'operations':1}])
            self.assertEqual(store.export(db),exported)


CRASH=textwrap.dedent('''\
    import json, os, sys
    sys.path.insert(0,sys.argv[4])
    from change_replay import store
    batch=json.load(open(sys.argv[1],encoding='utf-8'))
    point=sys.argv[2]
    if point=='before':store.ingest(sys.argv[3],batch,before_commit=lambda:os._exit(9))
    else:store.ingest(sys.argv[3],batch,after_commit=lambda:os._exit(9))
''')


class CrashTests(unittest.TestCase):
    """Kill a separate process mid-batch and prove only one version is visible."""

    def run_crash(self,script,batch,point,db,td):
        payload=Path(td)/'batch.json';payload.write_text(json.dumps(batch))
        return subprocess.run([sys.executable,'-I',str(script),str(payload),point,str(db),
                               str(ROOT/'src')],capture_output=True)

    def scenario(self):
        t1=tx('T1',11,[op('orders','O1',o1(1000),o1(1500)),op('customers','C2',None,cust('West'))])
        t2=tx('T2',12,[op('orders','O2',None,{'customer_id':'C2','amount_cents':500})])
        future=tx('T3',20,[op('customers','C1',{'name':'East'},cust('South'))])
        return {'transactions':[t2,t1,future],'watermark':12}

    def test_kill_before_commit_leaves_the_old_version_and_retry_applies(self):
        with tempfile.TemporaryDirectory() as td:
            db=Path(td)/'state.db';setup(db)
            old=store.export(db)
            script=Path(td)/'crash.py';script.write_text(CRASH)
            result=self.run_crash(script,self.scenario(),'before',db,td)
            self.assertEqual(result.returncode,9,result.stderr)
            self.assertEqual(store.export(db),old)
            store.ingest(db,self.scenario())
            new=store.export(db)
            self.assertNotEqual(new,old)
            self.assertEqual((new['watermark'],new['last_applied_lsn']),(12,12))
            self.assertEqual(new['customer_order_cents'],{'C1':1500,'C2':500})

    def test_kill_after_commit_leaves_the_new_version_and_retry_is_idempotent(self):
        with tempfile.TemporaryDirectory() as td:
            db=Path(td)/'state.db';setup(db)
            old=store.export(db)
            script=Path(td)/'crash.py';script.write_text(CRASH)
            result=self.run_crash(script,self.scenario(),'after',db,td)
            self.assertEqual(result.returncode,9,result.stderr)
            new=store.export(db)
            self.assertNotEqual(new,old)
            self.assertEqual(new['customer_order_cents'],{'C1':1500,'C2':500})
            self.assertEqual(new['pending_transactions'],
                             [{'id':'T3','commit_lsn':20,'operations':1}])
            retry=store.ingest(db,self.scenario())
            self.assertEqual(retry['applied_transactions'],[])
            self.assertEqual(retry['duplicate_transactions'],3)
            self.assertEqual(store.export(db),new)
            # Applying the future transaction later is a separate durable batch.
            store.ingest(db,{'transactions':[],'watermark':20})
            self.assertEqual(store.export(db)['customer_order_cents'],{'C1':1500,'C2':500})
            self.assertEqual(store.export(db)['state']['customers']['C1'],{'name':'South'})

    def test_failed_batch_rolls_back_all_journal_rows(self):
        with tempfile.TemporaryDirectory() as td:
            db=Path(td)/'state.db';setup(db)
            old=store.export(db)
            good=tx('T1',11,[op('customers','C2',None,cust('West'))])
            late=tx('T2',12,[op('customers','C1',{'name':'East'},None),
                             op('orders','O1',None,o1(5))])
            with self.assertRaises(ValueError):
                store.ingest(db,{'transactions':[good,late],'watermark':12})
            self.assertEqual(store.export(db),old)
            # If T1 had survived it would be a duplicate now; it must arrive as new.
            result=store.ingest(db,{'transactions':[good],'watermark':11})
            self.assertEqual(result['duplicate_transactions'],0)
            self.assertEqual([a['id'] for a in result['applied_transactions']],['T1'])


class CLITests(unittest.TestCase):
    def cli(self,*args):
        command=[sys.executable,'-m','change_replay.cli',*[str(a) for a in args]]
        return subprocess.run(command,capture_output=True,text=True)

    def test_init_ingest_export_round_trip(self):
        with tempfile.TemporaryDirectory() as td:
            p=Path(td);db=p/'state.db';batch=p/'batch.json';out=p/'export.json'
            fixture=json.loads((ROOT/'fixtures/input.json').read_text())
            result=self.cli('init','--baseline',ROOT/'fixtures/input.json','--db',db)
            self.assertEqual(result.returncode,0,result.stderr)
            self.assertEqual(json.loads(result.stdout)['watermark'],10)
            batch.write_text(json.dumps({'transactions':fixture['transactions'],'watermark':12}))
            result=self.cli('ingest','--db',db,'--batch',batch)
            self.assertEqual(result.returncode,0,result.stderr)
            self.assertEqual(json.loads(result.stdout)['last_applied_lsn'],12)
            result=self.cli('export','--db',db,'--output',out)
            self.assertEqual(result.returncode,0,result.stderr)
            exported=json.loads(out.read_text())
            self.assertEqual(exported['customer_order_cents'],{'C2':500})
            self.assertEqual(exported['pending_transactions'],
                             [{'id':'T3','commit_lsn':15,'operations':1}])

    def test_cli_failures_exit_two_and_keep_prior_output(self):
        with tempfile.TemporaryDirectory() as td:
            p=Path(td);db=p/'state.db';out=p/'export.json'
            self.assertEqual(self.cli('init','--baseline',ROOT/'fixtures/input.json','--db',db).returncode,0)
            self.assertEqual(self.cli('export','--db',db,'--output',out).returncode,0)
            prior=out.read_bytes()
            bad_batch=p/'bad.json';bad_batch.write_text(json.dumps({'transactions':[{'id':'T1','commit_lsn':11,
                                                                                    'operations':[]}],'watermark':9}))
            bad_transaction=p/'bad_tx.json'
            bad_transaction.write_text(json.dumps({'transactions':[{'id':'T1','commit_lsn':11}],
                                                   'watermark':12}))
            for args in [('ingest','--db',db,'--batch',bad_batch,'--output',out),
                         ('ingest','--db',db,'--batch',bad_transaction,'--output',out),
                         ('ingest','--db',db,'--batch',p/'missing.json'),
                         ('export','--db',p/'missing.db','--output',out),
                         ('init','--baseline',ROOT/'fixtures/input.json','--db',db),
                         ('init','--baseline',p/'missing.json','--db',p/'other.db')]:
                self.assertEqual(self.cli(*args).returncode,2,args)
                self.assertEqual(out.read_bytes(),prior)
                self.assertFalse((p/'other.db').exists())
            for args in [('ingest','--db',db,'--batch',ROOT/'fixtures/batch.json','--output',db),
                         ('export','--db',db,'--output',db)]:
                self.assertEqual(self.cli(*args).returncode,2,args)
            # The database is intact after the rejected self-overwrite attempts.
            exported=self.cli('export','--db',db)
            self.assertEqual(exported.returncode,0,exported.stderr)
            self.assertEqual(json.loads(exported.stdout)['watermark'],10)

    def test_legacy_flags_are_unchanged(self):
        with tempfile.TemporaryDirectory() as td:
            p=Path(td);source=ROOT/'fixtures/input.json'
            plain=p/'plain.json';full=p/'full.json'
            self.assertEqual(self.cli('--source',source,'--output',plain).returncode,0)
            self.assertEqual(self.cli('--source',source,'--output',full,'--history').returncode,0)
            default=json.loads(plain.read_text());extended=json.loads(full.read_text())
            self.assertEqual(default['result'],analyze(json.loads(source.read_text())))
            history=extended['result'].pop('history')
            self.assertEqual(extended,default)
            self.assertTrue(history)
            # A database path is never mistaken for the legacy positional-free flags.
            self.assertEqual(self.cli('--source',source,'--output',p/'x.json','--history').returncode,0)
            self.assertEqual(self.cli('ingest','--db',p/'none.db','--batch',source).returncode,2)


if __name__=='__main__':unittest.main()
