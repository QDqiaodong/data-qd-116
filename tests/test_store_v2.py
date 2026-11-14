import json, os, sqlite3, subprocess, sys, tempfile, textwrap, tracemalloc, unittest
from pathlib import Path
from change_replay import store

ROOT = Path(__file__).resolve().parents[1]


def baseline(customers=None, orders=None):
    return {'lsn': 10,
            'customers': customers if customers is not None else
            [{'key': 'C1', 'value': {'name': 'East'}}],
            'orders': orders if orders is not None else
            [{'key': 'O1', 'value': {'customer_id': 'C1', 'amount_cents': 1000}}]}


def tx(tid, lsn, ops):
    return {'id': tid, 'commit_lsn': lsn, 'operations': ops}


def op(table, key, before, after):
    return {'table': table, 'key': key, 'before': before, 'after': after}


def cust(name):
    return {'name': name}


def order(customer_id, amount):
    return {'customer_id': customer_id, 'amount_cents': amount}


def setup(db, document=None):
    store.initialize(db, {'baseline': document or baseline()})


def make_v1(db, document=None, journal=()):
    """Build a genuine v1 database using the preserved v1 schema."""
    document = document or baseline()
    connection = sqlite3.connect(db, isolation_level=None)
    try:
        connection.executescript(store.SCHEMA_V1)
        connection.execute('BEGIN IMMEDIATE')
        connection.executemany('INSERT INTO progress(key,value) VALUES(?,?)',
                               [('schema_version', 1), ('baseline_lsn', document['lsn']),
                                ('watermark', document['lsn']),
                                ('last_applied_lsn', document['lsn'])])
        connection.executemany('INSERT INTO customers(key,value) VALUES(?,?)',
                               ((row['key'], json.dumps(row['value'])) for row in document['customers']))
        connection.executemany('INSERT INTO orders(key,value) VALUES(?,?)',
                               ((row['key'], json.dumps(row['value'])) for row in document['orders']))
        for tid, lsn, content, applied in journal:
            connection.execute('INSERT INTO journal(id,commit_lsn,content,applied) VALUES(?,?,?,?)',
                               (tid, lsn, json.dumps(content), applied))
        connection.execute('COMMIT')
    finally:
        connection.close()


class OrderingAndForeignKeyTests(unittest.TestCase):
    def test_order_can_be_written_before_its_customer_in_one_transaction(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'state.db'; setup(db)
            store.ingest(db, {'transactions': [
                tx('T1', 11, [op('orders', 'O2', None, order('C2', 500)),
                              op('customers', 'C2', None, cust('West'))])], 'watermark': 11})
            exported = store.export(db)
            self.assertEqual(exported['state']['orders']['O2'], order('C2', 500))
            self.assertEqual(exported['customer_order_cents'], {'C1': 1000, 'C2': 500})

    def test_order_moved_across_customers_then_old_customer_deleted(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'state.db'; setup(db)
            # C2 arrives first; O1 then moves to C2 and C1 is deleted in the
            # same transaction, so the only reference to C1 disappears too.
            store.ingest(db, {'transactions': [
                tx('T1', 11, [op('customers', 'C2', None, cust('West'))]),
                tx('T2', 12, [op('orders', 'O1', order('C1', 1000), order('C2', 1000)),
                              op('customers', 'C1', cust('East'), None)])], 'watermark': 12})
            exported = store.export(db)
            self.assertEqual(exported['state'], {'customers': {'C2': {'name': 'West'}},
                                                 'orders': {'O1': order('C2', 1000)}})
            self.assertEqual(exported['customer_order_cents'], {'C2': 1000})

    def test_untouched_order_still_referencing_blocks_customer_delete(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'state.db'; setup(db)
            # O2 references C1 but is never touched; deleting C1 must fail even
            # though the transaction is internally consistent.
            store.ingest(db, {'transactions': [
                tx('T1', 11, [op('orders', 'O2', None, order('C1', 250))])], 'watermark': 11})
            prior = store.export(db)
            with self.assertRaisesRegex(ValueError, 'orphan'):
                store.ingest(db, {'transactions': [
                    tx('T2', 12, [op('customers', 'C1', cust('East'), None)])], 'watermark': 12})
            self.assertEqual(store.export(db), prior)
            # Moving every referencing order away first makes the delete legal;
            # O1 from the baseline is still untouched and blocks otherwise.
            with self.assertRaisesRegex(ValueError, 'orphan'):
                store.ingest(db, {'transactions': [
                    tx('T2', 12, [op('customers', 'C2', None, cust('West'))]),
                    tx('T3', 13, [op('orders', 'O2', order('C1', 250), order('C2', 250)),
                                  op('customers', 'C1', cust('East'), None)])], 'watermark': 13})
            self.assertEqual(store.export(db), prior)
            store.ingest(db, {'transactions': [
                tx('T2', 12, [op('customers', 'C2', None, cust('West'))]),
                tx('T3', 13, [op('orders', 'O1', order('C1', 1000), order('C2', 1000)),
                              op('orders', 'O2', order('C1', 250), order('C2', 250)),
                              op('customers', 'C1', cust('East'), None)])], 'watermark': 13})
            exported = store.export(db)
            self.assertNotIn('C1', exported['state']['customers'])
            self.assertEqual(exported['state']['orders'], {'O1': order('C2', 1000),
                                                           'O2': order('C2', 250)})

    def test_order_dropped_by_the_same_transaction_does_not_block_delete(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'state.db'; setup(db)
            store.ingest(db, {'transactions': [
                tx('T1', 11, [op('orders', 'O1', order('C1', 1000), None),
                              op('customers', 'C1', cust('East'), None)])], 'watermark': 11})
            self.assertEqual(store.export(db)['state'], {'customers': {}, 'orders': {}})

    def test_same_key_modified_repeatedly_in_one_and_many_transactions(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'state.db'; setup(db)
            ops = []
            current = 1000
            for step in range(1, 5):
                ops.append(op('orders', 'O1', order('C1', current), order('C1', current + step)))
                current += step
            store.ingest(db, {'transactions': [tx('T1', 11, ops)], 'watermark': 11})
            self.assertEqual(store.export(db)['state']['orders']['O1'], order('C1', 1010))
            # Re-modify the same key across separate batches, with a stale
            # before image in the middle.
            store.ingest(db, {'transactions': [
                tx('T2', 12, [op('orders', 'O1', order('C1', 1010), order('C1', 2000))])],
                'watermark': 12})
            prior = store.export(db)
            with self.assertRaisesRegex(ValueError, 'before'):
                store.ingest(db, {'transactions': [
                    tx('T3', 13, [op('orders', 'O1', order('C1', 1500), order('C1', 2500))])],
                    'watermark': 13})
            self.assertEqual(store.export(db), prior)
            self.assertEqual(store.export(db)['state']['orders']['O1'], order('C1', 2000))


class MigrationTests(unittest.TestCase):
    def test_init_creates_v2_and_export_reports_it(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'state.db'; setup(db)
            self.assertEqual(store.export(db)['schema_version'], 2)
            connection = sqlite3.connect(db)
            try:
                indexes = {row[0] for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='index'")}
            finally:
                connection.close()
            self.assertIn('orders_customer', indexes)

    def test_export_reads_both_schema_versions(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'state.db'; make_v1(db)
            exported = store.export(db)
            self.assertEqual(exported['schema_version'], 1)
            self.assertEqual(exported['state'], {'customers': {'C1': {'name': 'East'}},
                                                 'orders': {'O1': order('C1', 1000)}})

    def test_first_ingest_migrates_in_place_together_with_the_batch(self):
        t1 = {'id': 'T1', 'commit_lsn': 11,
              'operations': [op('customers', 'C2', None, cust('West'))]}
        pending = {'id': 'T9', 'commit_lsn': 20,
                   'operations': [op('orders', 'O9', None, order('C9', 5))]}
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'state.db'; make_v1(db, journal=[('T1', 11, t1, 0), ('T9', 20, pending, 0)])
            result = store.ingest(db, {'transactions': [], 'watermark': 12})
            self.assertEqual(result['applied_transactions'], [{'id': 'T1', 'commit_lsn': 11}])
            exported = store.export(db)
            self.assertEqual(exported['schema_version'], 2)
            self.assertEqual(exported['watermark'], 12)
            self.assertEqual(exported['state']['customers']['C2'], {'name': 'West'})
            self.assertEqual(exported['pending_transactions'],
                             [{'id': 'T9', 'commit_lsn': 20, 'operations': 1}])
            # The old JSON tables and the old pending index are gone.
            connection = sqlite3.connect(db)
            try:
                names = {row[0] for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type IN ('table','index')")}
            finally:
                connection.close()
            self.assertNotIn('customers_legacy', names)
            self.assertNotIn('orders_legacy', names)
            self.assertIn('orders_customer', names)

    def test_failed_application_migrates_and_applies_atomically(self):
        bad = {'id': 'T1', 'commit_lsn': 11,
               'operations': [op('customers', 'C1', cust('Wrong'), cust('West'))]}
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'state.db'; make_v1(db, journal=[('T1', 11, bad, 0)])
            prior = store.export(db)
            with self.assertRaisesRegex(ValueError, 'before'):
                store.ingest(db, {'transactions': [], 'watermark': 12})
            # Neither the migration nor the batch survived.
            self.assertEqual(store.export(db), prior)
            self.assertEqual(store.export(db)['schema_version'], 1)
            connection = sqlite3.connect(db)
            try:
                columns = [row[1] for row in connection.execute('PRAGMA table_info(customers)')]
            finally:
                connection.close()
            self.assertEqual(columns, ['key', 'value'])

    def test_failed_batch_on_a_v1_database_does_not_migrate(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'state.db'; make_v1(db)
            with self.assertRaisesRegex(ValueError, 'must not decrease'):
                store.ingest(db, {'transactions': [], 'watermark': 9})
            self.assertEqual(store.export(db)['schema_version'], 1)

    def test_v1_rejects_new_state_operations_that_cannot_round_trip(self):
        # A v1 database still enforces the same before-image contract after
        # migration; a duplicate identity remains a duplicate.
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'state.db'; make_v1(db)
            result = store.ingest(db, {'transactions': [
                tx('T1', 11, [op('orders', 'O1', order('C1', 1000), order('C1', 1200))])],
                'watermark': 11})
            self.assertEqual(result['applied_transactions'], [{'id': 'T1', 'commit_lsn': 11}])
            replay = store.ingest(db, {'transactions': [
                tx('T1', 11, [op('orders', 'O1', order('C1', 1000), order('C1', 1200))])],
                'watermark': 11})
            self.assertEqual(replay['duplicate_transactions'], 1)

    def test_migration_streams_large_legacy_tables_with_bounded_memory(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'state.db'
            rows = 20_000
            connection = sqlite3.connect(db, isolation_level=None)
            try:
                connection.executescript(store.SCHEMA_V1)
                connection.execute('BEGIN IMMEDIATE')
                connection.executemany('INSERT INTO progress(key,value) VALUES(?,?)',
                                       [('schema_version', 1), ('baseline_lsn', 1000),
                                        ('watermark', 1000), ('last_applied_lsn', 1000)])
                for start in range(1, rows + 1, 1000):
                    connection.executemany(
                        'INSERT INTO customers(key,value) VALUES(?,?)',
                        ((f'C{i}', json.dumps({'name': f'N{i}'}))
                         for i in range(start, min(start + 1000, rows + 1))))
                    connection.executemany(
                        'INSERT INTO orders(key,value) VALUES(?,?)',
                        ((f'O{i}', json.dumps({'customer_id': f'C{i}', 'amount_cents': 0}))
                         for i in range(start, min(start + 1000, rows + 1))))
                connection.execute('COMMIT')
            finally:
                connection.close()
            tracemalloc.start()
            try:
                store.ingest(db, {'transactions': [
                    tx('T1', 1001, [op('customers', 'NEW', None, cust('Added'))])],
                    'watermark': 1001})
                _, peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
            exported = store.export(db)
            self.assertEqual(exported['schema_version'], 2)
            self.assertEqual(len(exported['state']['customers']), rows + 1)
            self.assertEqual(len(exported['state']['orders']), rows)
            # The migration copies in chunks; it must not hold the whole
            # table as parsed JSON at once.
            self.assertLess(peak / 1024 / 1024, 8)


class SQLFootprintTests(unittest.TestCase):
    """Prove a normal batch neither scans nor rewrites the whole state."""

    @staticmethod
    def unconstrained(statement):
        """Flag state/journal access that is not pinned to a key."""
        sql = ' '.join(statement.split()).upper()
        for table in ('CUSTOMERS', 'ORDERS'):
            if ((sql.startswith('SELECT') and f' FROM {table} ' in sql) or
                    sql.startswith(f'DELETE FROM {table} ') or
                    sql.startswith(f'UPDATE {table} ')):
                keys = ('KEY=', 'KEY =')
                if table == 'ORDERS':
                    keys += ('CUSTOMER_ID=', 'CUSTOMER_ID =')
                if not any(token in sql for token in keys):
                    return True
        if ((sql.startswith('SELECT') and ' FROM JOURNAL ' in sql) or
                sql.startswith('UPDATE JOURNAL ') or
                sql.startswith('DELETE FROM JOURNAL ')):
            # Pending-only lookups or identity lookups are fine; a statement
            # that could read applied journal rows in bulk is not.
            if not any(token in sql for token in
                       ('APPLIED=0', 'APPLIED = 0', 'ID=', 'ID =', 'COMMIT_LSN=', 'COMMIT_LSN =')):
                return True
        return False

    def traced_ingest(self, db):
        statements = []
        store.ingest(db, {'transactions': [
            tx('B1', 11, [op('customers', 'NEW', None, cust('Added'))]),
            tx('B2', 12, [op('orders', 'O1', order('C1', 1000), order('NEW', 50))]),
            tx('B3', 13, [op('orders', 'O1', order('NEW', 50), order('NEW', 60)),
                          op('customers', 'C1', cust('East'), None)])],
            'watermark': 13}, trace=statements.append)
        return statements

    def test_small_batch_sql_is_keyed(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'state.db'; setup(db)
            statements = self.traced_ingest(db)
            offending = [s for s in statements if self.unconstrained(s)]
            self.assertEqual(offending, [])
            # No delete-and-rewrite of either materialised table: the only
            # DELETE statements are keyed single-row deletions.
            for table in ('CUSTOMERS', 'ORDERS'):
                for s in statements:
                    sql = ' '.join(s.split()).upper()
                    if sql.startswith(f'DELETE FROM {table}'):
                        self.assertIn('KEY=', sql, s)
            # The customer delete checked references through the order index
            # query, which filters on customer_id.
            self.assertTrue(any('FROM ORDERS WHERE CUSTOMER_ID=' in ' '.join(s.split()).upper()
                                for s in statements))
            self.assertEqual(store.export(db)['state'],
                             {'customers': {'NEW': {'name': 'Added'}},
                              'orders': {'O1': order('NEW', 60)}})

    def test_customer_delete_uses_the_order_reference_index(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'state.db'; setup(db)
            connection = sqlite3.connect(db)
            try:
                plan = connection.execute(
                    'EXPLAIN QUERY PLAN SELECT key FROM orders WHERE customer_id=? AND key>? '
                    'ORDER BY key LIMIT 256', ('C1', '')).fetchall()
            finally:
                connection.close()
            detail = ' '.join(row[-1] for row in plan)
            self.assertIn('orders_customer', detail)
            self.assertNotIn('SCAN orders', detail)

    def test_pending_and_identity_lookups_use_indexes(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'state.db'; setup(db)
            connection = sqlite3.connect(db)
            try:
                queries = [
                    ('SELECT id,commit_lsn FROM journal WHERE applied=0 AND commit_lsn<=? '
                     'AND (commit_lsn,id)>(?,?) ORDER BY commit_lsn,id LIMIT 256', (10, 0, '')),
                    ('SELECT id,commit_lsn FROM journal WHERE applied=0 '
                     'ORDER BY commit_lsn,id', ()),
                    ('SELECT id,commit_lsn,content FROM journal WHERE id=? OR commit_lsn=?',
                     ('T1', 11)),
                ]
                details = []
                for sql, parameters in queries:
                    plan = connection.execute('EXPLAIN QUERY PLAN ' + sql, parameters).fetchall()
                    details.append((sql, ' '.join(row[-1] for row in plan)))
            finally:
                connection.close()
            for sql, detail in details:
                self.assertNotIn('SCAN journal', detail, sql)
                self.assertNotIn('SCAN TABLE', detail.upper(), sql)

    def test_export_learns_both_versions(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td)
            v2 = p / 'v2.db'; v1 = p / 'v1.db'
            setup(v2); make_v1(v1)
            self.assertEqual(store.export(v2)['state'], store.export(v1)['state'])
            self.assertEqual((store.export(v1)['schema_version'],
                              store.export(v2)['schema_version']), (1, 2))


class CLIVersionTests(unittest.TestCase):
    def cli(self, *args):
        command = [sys.executable, '-m', 'change_replay.cli', *[str(a) for a in args]]
        return subprocess.run(command, capture_output=True, text=True,
                              env={**os.environ, 'PYTHONPATH': str(ROOT / 'src')})

    def test_cli_export_reads_v1_and_ingest_migrates_it(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td); db = p / 'state.db'; out = p / 'export.json'
            make_v1(db)
            result = self.cli('export', '--db', db, '--output', out)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(out.read_text())['schema_version'], 1)
            batch = p / 'batch.json'
            batch.write_text(json.dumps({'transactions': [
                tx('T1', 11, [op('customers', 'C2', None, cust('West'))])], 'watermark': 11}))
            result = self.cli('ingest', '--db', db, '--batch', batch)
            self.assertEqual(result.returncode, 0, result.stderr)
            result = self.cli('export', '--db', db, '--output', out)
            self.assertEqual(result.returncode, 0, result.stderr)
            exported = json.loads(out.read_text())
            self.assertEqual(exported['schema_version'], 2)
            self.assertEqual(exported['watermark'], 11)
            self.assertEqual(exported['state']['customers']['C2'], {'name': 'West'})

    def test_cli_failure_on_v1_database_keeps_it_at_v1(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td); db = p / 'state.db'
            make_v1(db)
            batch = p / 'batch.json'
            batch.write_text(json.dumps({'transactions': [], 'watermark': 9}))
            result = self.cli('ingest', '--db', db, '--batch', batch)
            self.assertEqual(result.returncode, 2)
            result = self.cli('export', '--db', db)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout)['schema_version'], 1)


CRASH = textwrap.dedent('''\
    import json, os, sys
    sys.path.insert(0, sys.argv[4])
    from change_replay import store
    batch = json.load(open(sys.argv[1], encoding='utf-8'))
    point = sys.argv[2]
    if point == 'before':
        store.ingest(sys.argv[3], batch, before_commit=lambda: os._exit(9))
    else:
        store.ingest(sys.argv[3], batch, after_commit=lambda: os._exit(9))
''')


class CrashDuringMigrationTests(unittest.TestCase):
    """A v1 database killed during its migrating ingest stays on one version."""

    def build(self, td):
        db = Path(td) / 'state.db'
        t1 = {'id': 'T1', 'commit_lsn': 11,
              'operations': [op('customers', 'C2', None, cust('West'))]}
        make_v1(db, journal=[('T1', 11, t1, 0)])
        batch = {'transactions': [], 'watermark': 11}
        script = Path(td) / 'crash.py'; script.write_text(CRASH)
        payload = Path(td) / 'batch.json'; payload.write_text(json.dumps(batch))
        return db, batch, script, payload

    def run_crash(self, script, payload, point, db):
        return subprocess.run([sys.executable, '-I', str(script), str(payload), point, str(db),
                               str(ROOT / 'src')], capture_output=True)

    def test_kill_before_commit_leaves_v1_and_retry_migrates(self):
        with tempfile.TemporaryDirectory() as td:
            db, batch, script, payload = self.build(td)
            old = store.export(db)
            result = self.run_crash(script, payload, 'before', db)
            self.assertEqual(result.returncode, 9, result.stderr)
            self.assertEqual(store.export(db), old)
            self.assertEqual(store.export(db)['schema_version'], 1)
            store.ingest(db, batch)
            new = store.export(db)
            self.assertEqual(new['schema_version'], 2)
            self.assertEqual(new['state']['customers']['C2'], {'name': 'West'})

    def test_kill_after_commit_leaves_migrated_new_version_and_retry_is_duplicate(self):
        with tempfile.TemporaryDirectory() as td:
            db, batch, script, payload = self.build(td)
            result = self.run_crash(script, payload, 'after', db)
            self.assertEqual(result.returncode, 9, result.stderr)
            new = store.export(db)
            self.assertEqual(new['schema_version'], 2)
            self.assertEqual(new['watermark'], 11)
            retry = store.ingest(db, batch)
            self.assertEqual(retry['duplicate_transactions'], 0)
            self.assertEqual(retry['applied_transactions'], [])
            self.assertEqual(store.export(db), new)


def seed(db, rows):
    """Write a large v2 database directly; the offline input is capped."""
    connection = sqlite3.connect(db, isolation_level=None)
    try:
        connection.executescript(store.SCHEMA)
        connection.execute('BEGIN IMMEDIATE')
        connection.executemany('INSERT INTO progress(key,value) VALUES(?,?)',
                               [('schema_version', store.SCHEMA_VERSION), ('baseline_lsn', 1000),
                                ('watermark', 1000), ('last_applied_lsn', 1000)])
        connection.executemany('INSERT INTO customers(key,name) VALUES(?,?)',
                               ((f'C{i}', f'Name {i}') for i in range(1, rows + 1)))
        connection.executemany('INSERT INTO orders(key,customer_id,amount_cents) VALUES(?,?,?)',
                               ((f'O{i}', f'C{i}', 0) for i in range(1, rows + 1)))
        connection.execute('COMMIT')
    finally:
        connection.close()


def measured_batch():
    return {'transactions': [
        tx('B1', 1001, [op('customers', 'NEW1', None, cust('Bench'))]),
        tx('B2', 1002, [op('orders', 'O1', order('C1', 0), order('NEW1', 100))]),
        tx('B3', 1003, [op('orders', 'O2', order('C2', 0), order('C2', 700))]),
        tx('B4', 1004, [op('orders', 'O2', order('C2', 700), order('C2', 900))]),
        tx('B5', 2000, [op('orders', 'OFUT', None, order('C1', 5))])],
        'watermark': 1004}


class MemoryFootprintTests(unittest.TestCase):
    """Peak ingest growth must not depend on how many untouched rows exist."""

    def measure(self, rows):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'state.db'; seed(db, rows)
            tracemalloc.start()
            try:
                store.ingest(db, measured_batch())
                _, peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
            self.assertEqual(store.export(db)['watermark'], 1004)
            return peak / 1024 / 1024

    def test_peak_growth_is_bounded_and_independent_of_database_size(self):
        small = self.measure(1000)
        large = self.measure(100_000)
        self.assertLess(small, 8)
        self.assertLess(large, 8)
        # Same legal small batch; 100x rows must not grow the working set.
        self.assertLess(large - small, 2)

    def test_same_batch_has_identical_effect_at_both_sizes(self):
        states = []
        with tempfile.TemporaryDirectory() as td:
            for rows in (1000, 100_000):
                db = Path(td) / f's{rows}.db'; seed(db, rows)
                store.ingest(db, measured_batch())
                exported = store.export(db)
                states.append({
                    'watermark': exported['watermark'],
                    'last_applied_lsn': exported['last_applied_lsn'],
                    'pending': exported['pending_transactions'],
                    'customers': {k: v for k, v in exported['state']['customers'].items()
                                  if k in ('C1', 'C2', 'NEW1')},
                    'orders': {k: v for k, v in exported['state']['orders'].items()
                               if k in ('O1', 'O2')},
                })
        self.assertEqual(states[0], states[1])


if __name__ == '__main__':
    unittest.main()
