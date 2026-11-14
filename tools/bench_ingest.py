"""Build synthetic databases and measure one ingest batch with tracemalloc.

Usage (from the repository root, standard library only)::

    PYTHONPATH=src python3 tools/bench_ingest.py --rows 1000
    PYTHONPATH=src python3 tools/bench_ingest.py --rows 100000
    PYTHONPATH=src python3 tools/bench_ingest.py --rows 1000 --sql-trace

``--rows N`` seeds N customers and N orders (order O_i belongs to C_i), all
with zero prior activity.  The measured batch is the same small legal batch
for either size: it adds a customer, moves one order to that customer in a
separate transaction, changes one amount repeatedly (two transactions on the
same key) and stages one future transaction.  ``--sql-trace`` prints the
distinct SQL statements traced during the measured ingest and flags any
statement that touches a state table or the journal without a key.

The reported ``peak_mib`` is the tracemalloc peak during the ingest call.
``export_peak_mib`` (only with ``--sql-trace``) measures a full export for
context; export is documented to be O(database size) and is not the batch
path.

Seed data is written directly through the v2 schema (the offline baseline
document is capped at 20000 rows that may arrive in a batch); the measured
ingest is the same code path as production and is the only work inside
tracemalloc.
"""
import argparse, json, re, sqlite3, tempfile, tracemalloc
from pathlib import Path

from change_replay import store

BATCH_TEMPLATE = {
    'transactions': [
        {'id': 'B1', 'commit_lsn': 1001,
         'operations': [{'table': 'customers', 'key': 'NEW1', 'before': None,
                         'after': {'name': 'Bench added'}}]},
        {'id': 'B2', 'commit_lsn': 1002,
         'operations': [{'table': 'orders', 'key': 'O1',
                         'before': {'customer_id': 'C1', 'amount_cents': 0},
                         'after': {'customer_id': 'NEW1', 'amount_cents': 100}}]},
        {'id': 'B3', 'commit_lsn': 1003,
         'operations': [{'table': 'orders', 'key': 'O2',
                         'before': {'customer_id': 'C2', 'amount_cents': 0},
                         'after': {'customer_id': 'C2', 'amount_cents': 700}}]},
        {'id': 'B4', 'commit_lsn': 1004,
         'operations': [{'table': 'orders', 'key': 'O2',
                         'before': {'customer_id': 'C2', 'amount_cents': 700},
                         'after': {'customer_id': 'C2', 'amount_cents': 900}}]},
    ],
    'watermark': 1004,
}
FUTURE = {'id': 'B5', 'commit_lsn': 2000,
          'operations': [{'table': 'orders', 'key': 'OFUT', 'before': None,
                          'after': {'customer_id': 'C1', 'amount_cents': 5}}]}


def batch():
    value = json.loads(json.dumps(BATCH_TEMPLATE))
    value['transactions'].append(FUTURE)
    return value


def seed(rows):
    """Return a v2 database path seeded with ``rows`` customers and orders.

    The offline baseline document is capped at 20000 rows per table, so the
    bench writes the same schema directly; the database is still a regular
    output of the pipeline.
    """
    tmp = tempfile.mkdtemp(prefix='change-replay-bench-')
    db = Path(tmp) / 'state.db'
    connection = sqlite3.connect(db, isolation_level=None)
    try:
        connection.executescript(store.SCHEMA)
        connection.execute('BEGIN IMMEDIATE')
        connection.executemany('INSERT INTO progress(key,value) VALUES(?,?)',
                               [('schema_version', store.SCHEMA_VERSION),
                                ('baseline_lsn', 1000), ('watermark', 1000),
                                ('last_applied_lsn', 1000)])
        connection.executemany('INSERT INTO customers(key,name) VALUES(?,?)',
                               ((f'C{i}', f'Name {i}') for i in range(1, rows + 1)))
        connection.executemany('INSERT INTO orders(key,customer_id,amount_cents) VALUES(?,?,?)',
                               ((f'O{i}', f'C{i}', 0) for i in range(1, rows + 1)))
        connection.execute('COMMIT')
    finally:
        connection.close()
    return db


def unconstrained(statement):
    """True when a statement reads or rewrites a state table without a key.

    Traced SQLite statements have their parameters expanded, so a bound key
    shows up as ``key='C1'``.  Journal statements are accepted when they pin
    ``id``/``commit_lsn`` or restrict to ``applied=0``; scanning every applied
    journal row would show up here.
    """
    sql = re.sub(r'\s+', ' ', statement.strip()).upper()
    if not re.match(r'^(SELECT|DELETE FROM|UPDATE)\b', sql):
        return False
    for table in ('CUSTOMERS', 'ORDERS'):
        if re.match(rf'^(SELECT .* FROM|DELETE FROM|UPDATE) {table}\b', sql):
            if not re.search(r'\b(KEY|CUSTOMER_ID)\s*=', sql):
                return True
    if re.match(r'^(SELECT .* FROM|UPDATE) JOURNAL\b', sql):
        if not re.search(r'\b(APPLIED\s*=\s*0|ID\s*=|COMMIT_LSN\s*=)', sql):
            return True
    return False


def measurement(db, trace_sql=False):
    statements = []
    def tracer(sql):
        statements.append(sql.strip())
    tracemalloc.start()
    before = tracemalloc.take_snapshot()
    result = store.ingest(db, batch(), trace=tracer if trace_sql else None)
    after = tracemalloc.take_snapshot()
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    growth = sum(stat.size_diff for stat in after.compare_to(before, 'filename')
                 if stat.size_diff > 0)
    return result, peak / 1024 / 1024, growth / 1024 / 1024, statements

def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--rows', type=int, required=True,
                        help='customers and orders to seed (1000 or 100000)')
    parser.add_argument('--sql-trace', action='store_true',
                        help='print the SQL traced during the measured ingest')
    args = parser.parse_args(argv)
    db = seed(args.rows)
    # Record the schema-to-database size before measuring.
    seeded_bytes = db.stat().st_size
    result, peak_mib, growth_mib, statements = measurement(db, args.sql_trace)
    exported = store.export(db)
    report = {
        'rows': args.rows,
        'peak_mib': round(peak_mib, 3),
        'growth_mib': round(growth_mib, 3),
        'watermark': result['watermark'],
        'last_applied_lsn': result['last_applied_lsn'],
        'pending': [p['id'] for p in exported['pending_transactions']],
        'database_bytes_before': seeded_bytes,
        'database_bytes_after': db.stat().st_size,
    }
    if args.sql_trace:
        unique = sorted({re.sub(r'\s+', ' ', s) for s in statements})
        report['sql_statements'] = unique
        report['unconstrained_statements'] = [s for s in unique if unconstrained(s)]
        report['trace_statement_count'] = len(statements)
        tracemalloc.start()
        try:
            store.export(db)
            _, export_peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        report['export_peak_mib'] = round(export_peak / 1024 / 1024, 3)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
