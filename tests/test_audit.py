import hashlib, json, os, sqlite3, subprocess, sys, tempfile, tracemalloc, unittest
from pathlib import Path
from change_replay import audit, store

ROOT = Path(__file__).resolve().parents[1]


def baseline():
    return {'lsn': 10,
            'customers': [{'key': 'C1', 'value': {'name': 'East'}}],
            'orders': [{'key': 'O1', 'value': {'customer_id': 'C1', 'amount_cents': 1000}}]}


def cust(name):
    return {'name': name}


def order(customer_id, amount):
    return {'customer_id': customer_id, 'amount_cents': amount}


def setup(db, document=None):
    store.initialize(db, {'baseline': document or baseline()})


def make_v1(db, customers, orders, watermark=10):
    connection = sqlite3.connect(db, isolation_level=None)
    try:
        connection.executescript(store.SCHEMA_V1)
        connection.execute('BEGIN IMMEDIATE')
        connection.executemany('INSERT INTO progress(key,value) VALUES(?,?)',
                               [('schema_version', 1), ('baseline_lsn', watermark),
                                ('watermark', watermark), ('last_applied_lsn', watermark)])
        connection.executemany('INSERT INTO customers(key,value) VALUES(?,?)',
                               ((key, json.dumps(value)) for key, value in customers.items()))
        connection.executemany('INSERT INTO orders(key,value) VALUES(?,?)',
                               ((key, json.dumps(value)) for key, value in orders.items()))
        connection.execute('COMMIT')
    finally:
        connection.close()


def make_v2(db, customers, orders, watermark=10):
    connection = sqlite3.connect(db, isolation_level=None)
    try:
        connection.executescript(store.SCHEMA)
        connection.execute('BEGIN IMMEDIATE')
        connection.executemany('INSERT INTO progress(key,value) VALUES(?,?)',
                               [('schema_version', 2), ('baseline_lsn', watermark),
                                ('watermark', watermark), ('last_applied_lsn', watermark)])
        connection.executemany('INSERT INTO customers(key,name) VALUES(?,?)',
                               ((key, value['name']) for key, value in customers.items()))
        connection.executemany('INSERT INTO orders(key,customer_id,amount_cents) VALUES(?,?,?)',
                               ((key, value['customer_id'], value['amount_cents'])
                                for key, value in orders.items()))
        connection.execute('COMMIT')
    finally:
        connection.close()


def reference_bytes(watermark, rows):
    lines = [json.dumps({'watermark': watermark}, separators=(',', ':'))]
    for table, key, value in rows:
        lines.append(json.dumps({'table': table, 'key': key, 'value': value},
                                separators=(',', ':')))
    return ('\n'.join(lines) + '\n').encode()


def write_reference(path, watermark, rows):
    raw = reference_bytes(watermark, rows)
    Path(path).write_bytes(raw)
    return raw


def baseline_rows():
    return [('customers', 'C1', cust('East')), ('orders', 'O1', order('C1', 1000))]


class ReferenceValidationTests(unittest.TestCase):
    def run_audit(self, td):
        db = Path(td) / 'state.db'
        setup(db)
        return db

    def assert_rejected(self, reference_text, pattern=None):
        with tempfile.TemporaryDirectory() as td:
            db = self.run_audit(td)
            ref = Path(td) / 'ref.ndjson'; ref.write_bytes(reference_text)
            if isinstance(pattern, str):
                with self.assertRaisesRegex(ValueError, pattern):
                    audit.run(db, ref)
            else:
                with self.assertRaises(ValueError):
                    audit.run(db, ref)

    def test_duplicate_keys_are_rejected_per_table_but_allowed_across_tables(self):
        lines = reference_bytes(10, baseline_rows() + [('customers', 'C1', cust('West'))])
        self.assert_rejected(lines, 'duplicate reference key')
        # The same key in customers and orders is not a duplicate.
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'state.db'
            make_v2(db, {'X': cust('Shared')}, {'X': order('X', 5)})
            ref = Path(td) / 'ref.ndjson'
            write_reference(ref, 10, [('orders', 'X', order('X', 5)),
                                      ('customers', 'X', cust('Shared'))])
            report = audit.run(db, ref)
            for table in audit.TABLES:
                for category in audit.CATEGORIES:
                    self.assertEqual(report['tables'][table][category]['count'], 0)

    def test_duplicate_json_fields_are_rejected(self):
        self.assert_rejected(
            b'{"watermark":10}\n{"table":"customers","key":"C1","value":{"name":"East"},'
            b'"value":{"name":"West"}}\n', 'duplicate JSON key')

    def test_bad_line_and_bad_last_line_are_rejected_with_line_numbers(self):
        header = b'{"watermark":10}\n'
        good = json.dumps({'table': 'customers', 'key': 'C2',
                           'value': {'name': 'West'}}, separators=(',', ':'))
        for payload, line in ((b'{\n', 1),
                              (header + good.encode() + b'\n{"table":"customers"}\n', 3),
                              (header + good.encode() + b'\n{"table":"customers"}', 3),
                              (header + b'[]\n', 2),
                              (header + b'\xff\n', 2),
                              (header + b'\n', 2),
                              (header + b'{}', 2)):
            self.assert_rejected(payload, f'line {line}')

    def test_header_must_be_first_and_have_exactly_one_watermark(self):
        self.assert_rejected(b'', 'empty reference')
        self.assert_rejected(b'{"table":"customers","key":"C1","value":{"name":"East"}}\n',
                             'line 1')
        self.assert_rejected(b'\n', 'line 1')
        self.assert_rejected(b'{"watermark":10,"extra":1}\n', 'line 1')
        self.assert_rejected(b'{"watermark":-1}\n', 'line 1')
        self.assert_rejected(b'{"watermark":10}\n{"watermark":10}\n', 'line 2')

    def test_orphan_orders_and_bad_values_are_rejected(self):
        header = b'{"watermark":10}\n'
        self.assert_rejected(header + b'{"table":"orders","key":"O9","value":'
                                      b'{"customer_id":"NOPE","amount_cents":1}}\n',
                             'orphan')
        self.assert_rejected(header + b'{"table":"ledger","key":"L1","value":{"name":"x"}}\n',
                             'line 2')
        self.assert_rejected(header + b'{"table":"orders","key":"O9","value":'
                                      b'{"customer_id":"C1","amount_cents":-1}}\n', 'line 2')
        self.assert_rejected(header + b'{"table":"customers","key":" ","value":{"name":"x"}}\n',
                             'line 2')

    def test_watermark_must_match_the_database(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'state.db'; setup(db)
            ref = Path(td) / 'ref.ndjson'
            write_reference(ref, 11, baseline_rows())
            prior = ref.read_bytes()
            with self.assertRaisesRegex(ValueError, 'watermark'):
                audit.run(db, ref)
            self.assertEqual(ref.read_bytes(), prior)


class ComparisonTests(unittest.TestCase):
    def test_unordered_reference_is_joined_with_cross_table_keys(self):
        rows = [('customers', 'A', cust('Alpha')), ('customers', 'B', cust('Beta')),
                ('orders', 'A2', order('A', 7)), ('orders', 'B2', order('B', 8))]
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'state.db'
            make_v2(db, {'A': cust('Alpha'), 'B': cust('Beta')},
                    {'A2': order('A', 7), 'B2': order('B', 8)})
            one = Path(td) / 'one.ndjson'; write_reference(one, 10, rows)
            shuffled = Path(td) / 'shuffled.ndjson'
            write_reference(shuffled, 10, [rows[3], rows[1], rows[2], rows[0]])
            report_one = audit.run(db, one); report_shuffled = audit.run(db, shuffled)
            self.assertEqual(report_one['tables'], report_shuffled['tables'])
            self.assertEqual(report_one['progress'], report_shuffled['progress'])
            self.assertNotEqual(report_one['source_sha256'], report_shuffled['source_sha256'])
            self.assertEqual(report_one['source_sha256'],
                             hashlib.sha256(one.read_bytes()).hexdigest())

    def test_exact_counts_and_sorted_twenty_key_samples(self):
        db_customers = {}; ref_rows = []
        for prefix in ('D', 'R', 'V'):
            for n in range(1, 26):
                key = f'{prefix}{n:03d}'
                if prefix in ('D', 'V'):
                    db_customers[key] = cust(f'db {key}')
                if prefix in ('R', 'V'):
                    ref_rows.append(('customers', key,
                                     cust(f'ref {key}' if prefix == 'R' else 'ref value')))
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'state.db'
            make_v2(db, db_customers, {})
            ref = Path(td) / 'ref.ndjson'
            write_reference(ref, 10, list(reversed(ref_rows)))
            report = audit.run(db, ref)
            customers = report['tables']['customers']
            self.assertEqual(customers['db_only']['count'], 25)
            self.assertEqual(customers['reference_only']['count'], 25)
            self.assertEqual(customers['value_differs']['count'], 25)
            for category, prefix in (('db_only', 'D'), ('reference_only', 'R'),
                                     ('value_differs', 'V')):
                samples = customers[category]['samples']
                self.assertEqual(len(samples), audit.SAMPLE_LIMIT)
                self.assertEqual([sample['key'] for sample in samples],
                                 [f'{prefix}{n:03d}' for n in range(1, 21)])
            sample = customers['value_differs']['samples'][0]
            self.assertEqual((sample['db'], sample['reference']),
                             (cust('db V001'), cust('ref value')))
            self.assertIsNone(customers['db_only']['samples'][0]['reference'])
            self.assertIsNone(customers['reference_only']['samples'][0]['db'])
            self.assertEqual(report['progress'],
                             {'schema_version': 2, 'baseline_lsn': 10,
                              'watermark': 10, 'last_applied_lsn': 10})

    def test_v1_and_v2_databases_produce_the_same_table_report(self):
        customers = {'C1': cust('East'), 'C2': cust('West')}
        orders = {'O1': order('C1', 100), 'O2': order('C2', 200)}
        rows = ([('customers', key, value) for key, value in customers.items()] +
                [('orders', key, value) for key, value in orders.items()])
        with tempfile.TemporaryDirectory() as td:
            p = Path(td); v1 = p / 'v1.db'; v2 = p / 'v2.db'; ref = p / 'ref.ndjson'
            make_v1(v1, customers, orders); make_v2(v2, customers, orders)
            write_reference(ref, 10, rows)
            report_v1 = audit.run(v1, ref); report_v2 = audit.run(v2, ref)
            self.assertEqual(report_v1['tables'], report_v2['tables'])
            self.assertEqual((report_v1['progress']['schema_version'],
                              report_v2['progress']['schema_version']), (1, 2))
            for table in audit.TABLES:
                for category in audit.CATEGORIES:
                    self.assertEqual(report_v2['tables'][table][category]['count'], 0)

    def test_v1_raw_json_is_compared_canonically(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'state.db'
            make_v1(db, {'C1': cust('East')}, {'O1': order('C1', 1000)})
            connection = sqlite3.connect(db, isolation_level=None)
            try:
                # Different field order and whitespace in the stored JSON text
                # is the same row, not a difference.
                connection.execute(
                    "UPDATE orders SET value=? WHERE key='O1'",
                    (json.dumps({'amount_cents': 1000, 'customer_id': 'C1'}, indent=1),))
                connection.commit()
            finally:
                connection.close()
            ref = Path(td) / 'ref.ndjson'
            write_reference(ref, 10, baseline_rows())
            report = audit.run(db, ref)
            self.assertEqual(report['tables']['orders']['value_differs']['count'], 0)

    def test_differences_are_a_normal_report_not_an_error(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'state.db'; setup(db)
            ref = Path(td) / 'ref.ndjson'; write_reference(ref, 10, [])
            report = audit.run(db, ref)
            self.assertEqual(report['tables']['customers']['db_only']['count'], 1)
            self.assertEqual(report['tables']['orders']['db_only']['count'], 1)
            self.assertEqual(report['tables']['customers']['db_only']['samples'],
                             [{'key': 'C1', 'db': cust('East'), 'reference': None}])


class ReadOnlyAndSnapshotTests(unittest.TestCase):
    def test_audit_never_migrates_or_rewrites_a_v1_database(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'state.db'; make_v1(db, {'C1': cust('East')},
                                               {'O1': order('C1', 1000)})
            ref = Path(td) / 'ref.ndjson'; write_reference(ref, 10, baseline_rows())
            before = hashlib.sha256(db.read_bytes()).hexdigest()
            files = sorted(p.name for p in Path(td).iterdir())
            audit.run(db, ref)
            self.assertEqual(hashlib.sha256(db.read_bytes()).hexdigest(), before)
            self.assertEqual(sorted(p.name for p in Path(td).iterdir()), files)
            self.assertEqual(store.export(db)['schema_version'], 1)

    def test_wal_concurrent_ingest_commit_does_not_mix_snapshots(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'state.db'; setup(db)
            connection = sqlite3.connect(db, isolation_level=None)
            try:
                self.assertEqual(connection.execute('PRAGMA journal_mode=WAL').fetchone(),
                                 ('wal',))
            finally:
                connection.close()
            ref = Path(td) / 'ref.ndjson'; write_reference(ref, 10, baseline_rows())
            observed = {}
            batch = {'transactions': [
                {'id': 'T1', 'commit_lsn': 11, 'operations': [
                    {'table': 'customers', 'key': 'C2', 'before': None,
                     'after': cust('West')},
                    {'table': 'orders', 'key': 'O2', 'before': None,
                     'after': order('C2', 500)}]}], 'watermark': 11}

            def interleave():
                result = store.ingest(db, batch)
                observed['ingest'] = result

            report = audit.run(db, ref, after_begin=interleave)
            self.assertEqual(observed['ingest']['applied_transactions'],
                             [{'id': 'T1', 'commit_lsn': 11}])
            # The report is entirely pre-commit: neither progress nor tables
            # may come from the version written half-way through the audit.
            self.assertEqual(report['progress']['watermark'], 10)
            for table in audit.TABLES:
                for category in audit.CATEGORIES:
                    self.assertEqual(report['tables'][table][category]['count'], 0)
            exported = store.export(db)
            self.assertEqual(exported['watermark'], 11)
            self.assertIn('C2', exported['state']['customers'])

    def test_concurrent_v1_migration_commit_is_not_seen_half_way(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'state.db'
            make_v1(db, {'C1': cust('East')}, {'O1': order('C1', 1000)})
            connection = sqlite3.connect(db, isolation_level=None)
            try:
                connection.execute('PRAGMA journal_mode=WAL')
            finally:
                connection.close()
            ref = Path(td) / 'ref.ndjson'; write_reference(ref, 10, baseline_rows())
            migrated = {}

            def interleave():
                # The first ingest migrates the v1 schema in place; the audit
                # must keep reading the old JSON tables and old progress.
                migrated.update(store.ingest(db, {'transactions': [], 'watermark': 10}))

            report = audit.run(db, ref, after_begin=interleave)
            self.assertEqual(migrated['watermark'], 10)
            self.assertEqual(report['progress']['schema_version'], 1)
            for table in audit.TABLES:
                for category in audit.CATEGORIES:
                    self.assertEqual(report['tables'][table][category]['count'], 0)
            self.assertEqual(store.export(db)['schema_version'], 2)

    def test_wal_read_sees_uncheckpointed_rows_without_rewriting_the_main_file(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'state.db'
            make_v2(db, {}, {})
            writer = sqlite3.connect(db, isolation_level=None)
            try:
                writer.execute('PRAGMA journal_mode=WAL')
                writer.execute('PRAGMA wal_autocheckpoint=0')
                writer.execute('BEGIN IMMEDIATE')
                writer.execute("INSERT INTO customers(key,name) VALUES('C1','East')")
                writer.execute('COMMIT')
                wal = Path(td) / 'state.db-wal'
                self.assertTrue(wal.exists())
                ref = Path(td) / 'ref.ndjson'
                write_reference(ref, 10, [('customers', 'C1', cust('East'))])
                before = db.read_bytes()
                wal_before = wal.read_bytes()
                report = audit.run(db, ref)
                self.assertEqual(report['tables']['customers']['reference_only']['count'], 0)
                self.assertEqual(db.read_bytes(), before)
                self.assertEqual(wal.read_bytes(), wal_before)
            finally:
                writer.close()

    def test_foreign_or_missing_database_is_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td); ref = p / 'ref.ndjson'; write_reference(ref, 10, baseline_rows())
            with self.assertRaisesRegex(ValueError, 'not initialized'):
                audit.run(p / 'missing.db', ref)
            foreign = p / 'foreign.db'
            connection = sqlite3.connect(foreign)
            connection.execute('CREATE TABLE other(x)'); connection.commit(); connection.close()
            with self.assertRaisesRegex(ValueError, 'not initialized'):
                audit.run(foreign, ref)

    def test_temporary_reference_database_is_cleaned_up_on_failure_and_success(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'state.db'; setup(db)
            ref = Path(td) / 'ref.ndjson'
            write_reference(ref, 10, baseline_rows() + [('customers', 'C1', cust('West'))])
            old = tempfile.tempdir; tempfile.tempdir = td
            try:
                with self.assertRaises(ValueError):
                    audit.run(db, ref)
                self.assertEqual(list(Path(td).glob('.change-replay-audit-*')), [])
                write_reference(ref, 10, baseline_rows())
                audit.run(db, ref)
                self.assertEqual(list(Path(td).glob('.change-replay-audit-*')), [])
            finally:
                tempfile.tempdir = old


class CLITests(unittest.TestCase):
    def cli(self, *args, env=None):
        command = [sys.executable, '-m', 'change_replay.cli', *[str(a) for a in args]]
        return subprocess.run(command, capture_output=True, text=True,
                              env={**os.environ, 'PYTHONPATH': str(ROOT / 'src'),
                                   **(env or {})})

    def test_audit_cli_publishes_differences_and_keeps_old_report_on_bad_input(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td); db = p / 'state.db'; ref = p / 'ref.ndjson'; out = p / 'report.json'
            setup(db)
            write_reference(ref, 10, [])
            result = self.cli('audit', '--db', db, '--reference', ref, '--output', out)
            self.assertEqual(result.returncode, 0, result.stderr)
            published = json.loads(out.read_text())
            self.assertEqual(published, audit.run(db, ref))
            self.assertEqual(published['tables']['customers']['db_only']['count'], 1)

            before = out.read_bytes()
            ref.write_bytes(reference_bytes(10, baseline_rows())[:-1] + b'{"table":\n')
            result = self.cli('audit', '--db', db, '--reference', ref, '--output', out,
                              env={'TMPDIR': td})
            self.assertEqual(result.returncode, 2)
            self.assertIn('line 3', result.stderr)
            self.assertEqual(out.read_bytes(), before)
            self.assertEqual(list(p.glob('.change-replay-audit-*')), [])

            write_reference(ref, 11, baseline_rows())
            result = self.cli('audit', '--db', db, '--reference', ref, '--output', out)
            self.assertEqual(result.returncode, 2)
            self.assertIn('watermark', result.stderr)
            self.assertEqual(out.read_bytes(), before)

    def test_audit_cli_argument_paths_must_differ_and_db_is_untouched(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td); db = p / 'state.db'; ref = p / 'ref.ndjson'
            setup(db); write_reference(ref, 10, baseline_rows())
            before = db.read_bytes()
            self.assertEqual(self.cli('audit', '--db', db, '--reference', ref,
                                      '--output', db).returncode, 2)
            self.assertEqual(self.cli('audit', '--db', db, '--reference', ref,
                                      '--output', ref).returncode, 2)
            self.assertEqual(db.read_bytes(), before)
            self.assertEqual(self.cli('audit', '--db', p / 'missing.db', '--reference', ref,
                                      '--output', p / 'out.json').returncode, 2)
            self.assertFalse((p / 'out.json').exists())
            self.assertEqual(self.cli('audit', '--db', db, '--reference', ref).returncode, 2)

    def test_failed_publication_keeps_old_report_and_cleans_temp_files(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td); db = p / 'state.db'; ref = p / 'ref.ndjson'
            outdir = p / 'read-only'; outdir.mkdir()
            out = outdir / 'report.json'; out.write_text('prior\n')
            setup(db); write_reference(ref, 10, baseline_rows())
            mode = outdir.stat().st_mode
            outdir.chmod(0o500)
            try:
                result = self.cli('audit', '--db', db, '--reference', ref,
                                  '--output', out, env={'TMPDIR': td})
            finally:
                outdir.chmod(mode)
            if os.geteuid() != 0:
                self.assertEqual(result.returncode, 2, result.stderr)
            if result.returncode != 0:
                self.assertEqual(out.read_text(), 'prior\n')
            self.assertEqual(list(p.glob('.change-replay-audit-*')), [])

    def test_legacy_entry_point_is_unchanged(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'report.json'
            result = self.cli('--source', ROOT / 'fixtures/input.json', '--output', out)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(out.read_text())['source_sha256'],
                             hashlib.sha256((ROOT / 'fixtures/input.json').read_bytes())
                             .hexdigest())


def seed(db, rows):
    connection = sqlite3.connect(db, isolation_level=None)
    try:
        connection.executescript(store.SCHEMA)
        connection.execute('BEGIN IMMEDIATE')
        connection.executemany('INSERT INTO progress(key,value) VALUES(?,?)',
                               [('schema_version', 2), ('baseline_lsn', 1000),
                                ('watermark', 1000), ('last_applied_lsn', 1000)])
        connection.executemany('INSERT INTO customers(key,name) VALUES(?,?)',
                               ((f'C{i}', f'Name {i}') for i in range(1, rows + 1)))
        connection.executemany('INSERT INTO orders(key,customer_id,amount_cents) VALUES(?,?,?)',
                               ((f'O{i}', f'C{i}', i) for i in range(1, rows + 1)))
        connection.execute('COMMIT')
    finally:
        connection.close()


def write_large_reference(path, rows):
    with Path(path).open('w', encoding='utf-8') as stream:
        stream.write('{"watermark":1000}\n')
        for i in range(rows, 0, -1):
            stream.write(json.dumps({'table': 'orders', 'key': f'O{i}',
                                     'value': order(f'C{i}', i)}, separators=(',', ':')) + '\n')
            stream.write(json.dumps({'table': 'customers', 'key': f'C{i}',
                                     'value': cust(f'Name {i}')},
                                    separators=(',', ':')) + '\n')


class ScaleTests(unittest.TestCase):
    def measure(self, rows):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'state.db'; ref = Path(td) / 'ref.ndjson'
            seed(db, rows); write_large_reference(ref, rows)
            size = ref.stat().st_size
            tracemalloc.start()
            try:
                report = audit.run(db, ref)
                _, peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
            self.assertEqual(list(Path(td).glob('.change-replay-audit-*')), [])
            self.assertEqual(list(Path(td).glob('*.db-shm')), [])
            self.assertEqual(list(Path(td).glob('*.db-wal')), [])
            for table in audit.TABLES:
                for category in audit.CATEGORIES:
                    self.assertEqual(report['tables'][table][category]['count'], 0)
            return peak / 1024 / 1024, size

    def test_100k_rows_above_eight_megabytes_with_bounded_memory(self):
        small, small_size = self.measure(1000)
        large, large_size = self.measure(100_000)
        self.assertGreater(large_size, 8_000_000)
        self.assertLess(small, 8)
        self.assertLess(large, 8)
        # 100x reference lines must not grow peak memory by anywhere near 8 MiB.
        self.assertLess(large - small, 8)


if __name__ == '__main__':
    unittest.main()
