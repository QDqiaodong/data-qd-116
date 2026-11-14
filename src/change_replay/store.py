"""SQLite-backed durable receive pipeline.

A database is initialised once from an existing baseline, then fed batched
transaction logs.  Every batch commits its identity/pending journal, every
materialised row it changed and the progress counters in one SQLite
transaction: a crash before the commit can only leave the previous version, a
crash after it can only leave the new one.  Applied transaction identities
stay in the database, so a restart never replays them and never rereads an
old source file.

Schema v2 stores one typed row per key and applies each transaction through a
private working set containing only the rows that transaction touches.  A
normal batch therefore never reads or rewrites the whole customer/order state
and never scans applied journal entries; ``orders.customer_id`` is indexed so
deleting a customer can detect a still-referencing order without scanning the
order table.

The watermark is a receive frontier, not application progress.  It means no
transaction with ``commit_lsn <= watermark`` will ever arrive for the first
time again; LSN holes are therefore permanent and ``last_applied_lsn`` may be
smaller than the watermark.  Transactions beyond the new watermark are kept
as pending after shape validation only; they are applied when a later batch
moves the watermark past their commit LSN.  A v1 database is migrated in place
by the first ingest, in the same transaction as that batch.
"""
from pathlib import Path
import json, os, sqlite3, tempfile

from .common import ident, number, require, rows
from .transaction import VALIDATE

SCHEMA_VERSION = 2
LEGACY_SCHEMA_VERSION = 1

_JOURNAL_PENDING = 'CREATE INDEX journal_pending ON journal(applied,commit_lsn,id)'
_STATE_STATEMENTS = (
    'CREATE TABLE customers(key TEXT PRIMARY KEY, name TEXT NOT NULL) WITHOUT ROWID',
    'CREATE TABLE orders(key TEXT PRIMARY KEY, customer_id TEXT NOT NULL, '
    'amount_cents INTEGER NOT NULL) WITHOUT ROWID',
    'CREATE INDEX orders_customer ON orders(customer_id,key)',
)

# Kept verbatim so tests (and operators migrating an old database) can build
# the previous generation.  v1 kept each row as a JSON document in ``value``.
SCHEMA_V1 = """\
CREATE TABLE progress(
    key TEXT PRIMARY KEY,
    value INTEGER NOT NULL
) WITHOUT ROWID;
CREATE TABLE journal(
    id TEXT PRIMARY KEY,
    commit_lsn INTEGER NOT NULL UNIQUE,
    content TEXT NOT NULL,
    applied INTEGER NOT NULL CHECK(applied IN (0,1))
) WITHOUT ROWID;
CREATE INDEX journal_pending ON journal(applied,commit_lsn);
CREATE TABLE customers(
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;
CREATE TABLE orders(
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;
"""

SCHEMA = ';\n'.join((
    'CREATE TABLE progress(key TEXT PRIMARY KEY, value INTEGER NOT NULL) WITHOUT ROWID',
    'CREATE TABLE journal(id TEXT PRIMARY KEY, commit_lsn INTEGER NOT NULL UNIQUE, '
    "content TEXT NOT NULL, applied INTEGER NOT NULL CHECK(applied IN (0,1))) WITHOUT ROWID",
    _JOURNAL_PENDING,
    *_STATE_STATEMENTS,
)) + ';\n'

BASELINE_FIELDS = 'lsn customers orders'


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def _baseline(document):
    """Extract and validate the baseline from a baseline or legacy input doc."""
    if not isinstance(document, dict):
        raise ValueError('expected baseline document')
    if 'baseline' in document:
        # Also accept the legacy input document; only its baseline is used.
        if set(document) not in ({'baseline'}, {'baseline', 'transactions', 'through_lsn'}):
            raise ValueError('unexpected baseline document fields')
        base = document['baseline']
    elif set(document) == set(BASELINE_FIELDS.split()):
        base = document
    else:
        raise ValueError('expected baseline object or input document')
    require(base, BASELINE_FIELDS)
    base_lsn = number(base['lsn'])
    state = {'customers': {}, 'orders': {}}
    for table, validator in (('customers', VALIDATE['customers']), ('orders', VALIDATE['orders'])):
        for row in rows(base[table]):
            require(row, 'key value'); key = ident(row['key'])
            if key in state[table]:
                raise ValueError('duplicate baseline key')
            state[table][key] = validator(row['value'])
    for value in state['orders'].values():
        if value['customer_id'] not in state['customers']:
            raise ValueError('baseline leaves orphan order')
    return base_lsn, state


def _sync(path):
    try:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass


def _connect(db_path):
    path = Path(db_path)
    if not path.exists():
        raise ValueError('database is not initialized')
    connection = sqlite3.connect(path, isolation_level=None)
    connection.execute('PRAGMA synchronous=FULL')
    try:
        row = connection.execute("SELECT value FROM progress WHERE key='schema_version'").fetchone()
    except sqlite3.DatabaseError:
        connection.close()
        raise ValueError('database is not initialized') from None
    if row is None or row[0] not in (LEGACY_SCHEMA_VERSION, SCHEMA_VERSION):
        connection.close()
        raise ValueError('unsupported database schema')
    return connection


def _progress(connection):
    return dict(connection.execute('SELECT key,value FROM progress'))


def _read_state(connection, schema_version):
    """Read all materialised rows; export is the only caller."""
    state = {'customers': {}, 'orders': {}}
    if schema_version == LEGACY_SCHEMA_VERSION:
        for key, value in connection.execute('SELECT key,value FROM customers ORDER BY key'):
            state['customers'][key] = json.loads(value)
        for key, value in connection.execute('SELECT key,value FROM orders ORDER BY key'):
            state['orders'][key] = json.loads(value)
    else:
        for key, name in connection.execute('SELECT key,name FROM customers ORDER BY key'):
            state['customers'][key] = {'name': name}
        for key, customer_id, amount in connection.execute(
                'SELECT key,customer_id,amount_cents FROM orders ORDER BY key'):
            state['orders'][key] = {'customer_id': customer_id, 'amount_cents': amount}
    return state


def _copy_legacy_rows(connection, table, insert_sql, convert):
    """Stream a v1 JSON table into its v2 typed table with bounded memory."""
    cursor = connection.execute(f'SELECT key,value FROM {table}')
    while True:
        chunk = cursor.fetchmany(512)
        if not chunk:
            return
        connection.executemany(insert_sql, [convert(key, raw) for key, raw in chunk])


def _legacy_customer(key, raw):
    value = VALIDATE['customers'](json.loads(raw))
    return key, value['name']


def _legacy_order(key, raw):
    value = VALIDATE['orders'](json.loads(raw))
    return key, value['customer_id'], value['amount_cents']


def _migrate_v1(connection):
    """Rewrite a v1 state representation into v2 inside the caller's transaction.

    The caller owns BEGIN/COMMIT; any failure here rolls the migration back
    together with the batch that triggered it.
    """
    connection.execute('ALTER TABLE customers RENAME TO customers_legacy')
    connection.execute('ALTER TABLE orders RENAME TO orders_legacy')
    connection.execute('DROP INDEX IF EXISTS journal_pending')
    for statement in _STATE_STATEMENTS:
        connection.execute(statement)
    _copy_legacy_rows(connection, 'customers_legacy',
                      'INSERT INTO customers(key,name) VALUES(?,?)', _legacy_customer)
    _copy_legacy_rows(connection, 'orders_legacy',
                      'INSERT INTO orders(key,customer_id,amount_cents) VALUES(?,?,?)',
                      _legacy_order)
    connection.execute('DROP TABLE customers_legacy')
    connection.execute('DROP TABLE orders_legacy')
    connection.execute(_JOURNAL_PENDING)
    connection.execute("UPDATE progress SET value=? WHERE key='schema_version'", (SCHEMA_VERSION,))


def initialize(db_path, document):
    """Create a new v2 database from an existing baseline; never overwrite."""
    base_lsn, state = _baseline(document)
    path = Path(db_path)
    if path.is_symlink() or path.exists():
        raise ValueError('database path already exists')
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temp = tempfile.mkstemp(prefix='.change-replay-init-', suffix='.db', dir=path.parent)
    os.close(handle); os.unlink(temp)
    try:
        connection = sqlite3.connect(temp, isolation_level=None)
        try:
            connection.execute('PRAGMA synchronous=FULL')
            connection.executescript(SCHEMA)
            connection.execute('BEGIN IMMEDIATE')
            try:
                connection.executemany('INSERT INTO progress(key,value) VALUES(?,?)',
                                       [('schema_version', SCHEMA_VERSION),
                                        ('baseline_lsn', base_lsn),
                                        ('watermark', base_lsn),
                                        ('last_applied_lsn', base_lsn)])
                connection.executemany(
                    'INSERT INTO customers(key,name) VALUES(?,?)',
                    ((key, value['name']) for key, value in state['customers'].items()))
                connection.executemany(
                    'INSERT INTO orders(key,customer_id,amount_cents) VALUES(?,?,?)',
                    ((key, value['customer_id'], value['amount_cents'])
                     for key, value in state['orders'].items()))
                connection.execute('COMMIT')
            except BaseException:
                connection.execute('ROLLBACK'); raise
        finally:
            connection.close()
        _sync(temp); os.replace(temp, path); _sync(path.parent)
    except BaseException:
        try:
            os.unlink(temp)
        except OSError:
            pass
        raise
    return {'baseline_lsn': base_lsn, 'watermark': base_lsn, 'last_applied_lsn': base_lsn,
            'customers': len(state['customers']), 'orders': len(state['orders'])}


def _transaction_record(transaction, base_lsn):
    """Validate one transaction object; return (id,commit_lsn,canonical)."""
    require(transaction, 'id commit_lsn operations')
    tid = ident(transaction['id'])
    lsn = number(transaction['commit_lsn'], base_lsn + 1)
    operations = rows(transaction['operations'], 1000)
    for operation in operations:
        require(operation, 'table key before after'); ident(operation['key'])
        table = operation['table']
        if not isinstance(table, str) or table not in VALIDATE:
            raise ValueError('unknown change table')
        before, after = operation['before'], operation['after']
        if before is None and after is None:
            raise ValueError('empty change operation')
        for value in (before, after):
            if value is not None:
                VALIDATE[table](value)
    return tid, lsn, _canonical(transaction)


class _WorkingSet:
    """Rows touched by one transaction, read from the database on demand.

    Untouched rows are never loaded.  A ``None`` value means "touched and
    absent"; membership of the dict distinguishes it from "not loaded".
    """

    __slots__ = ('connection', 'customers', 'orders')

    def __init__(self, connection):
        self.connection = connection
        self.customers = {}
        self.orders = {}

    def customer_name(self, key):
        if key in self.customers:
            return self.customers[key]
        row = self.connection.execute('SELECT name FROM customers WHERE key=?', (key,)).fetchone()
        return None if row is None else row[0]

    def order_value(self, key):
        if key in self.orders:
            return self.orders[key]
        return self.connection.execute(
            'SELECT customer_id,amount_cents FROM orders WHERE key=?', (key,)).fetchone()

    def image(self, table, key):
        if table == 'customers':
            name = self.customer_name(key)
            return None if name is None else {'name': name}
        order = self.order_value(key)
        return None if order is None else {'customer_id': order[0], 'amount_cents': order[1]}

    def set(self, table, key, after):
        if table == 'customers':
            self.customers[key] = None if after is None else after['name']
        else:
            self.orders[key] = None if after is None else (
                after['customer_id'], after['amount_cents'])


def _has_untouched_reference(connection, customer_id, touched_orders):
    """True if any order for this customer was not touched by the transaction.

    Uses the ``orders_customer(customer_id,key)`` index and pages by key.  In
    the common success case every matching order is in the working set, so
    the loop is bounded by the transaction size; an untouched reference is
    found on the first page.
    """
    last = ''
    while True:
        chunk = connection.execute(
            'SELECT key FROM orders WHERE customer_id=? AND key>? ORDER BY key LIMIT 256',
            (customer_id, last)).fetchall()
        if not chunk:
            return False
        for (key,) in chunk:
            if key not in touched_orders:
                return True
        last = chunk[-1][0]


def _apply_operations(connection, operations):
    """Apply one transaction's operations to a private working set, then flush.

    Operation order is preserved for before-image validation, an order may be
    written before its customer exists, and foreign keys are checked against
    the transaction's final state.  A customer removed by the transaction
    must not be referenced by an order the transaction did not touch.
    """
    working = _WorkingSet(connection)
    for operation in operations:
        require(operation, 'table key before after')
        table = operation['table']; key = ident(operation['key'])
        if not isinstance(table, str) or table not in VALIDATE:
            raise ValueError('unknown change table')
        before, after = operation['before'], operation['after']
        if before is None and after is None:
            raise ValueError('empty change operation')
        if before is not None:
            VALIDATE[table](before)
        if after is not None:
            VALIDATE[table](after)
        if working.image(table, key) != before:
            raise ValueError('before image does not match current row')
        working.set(table, key, after)

    # Foreign keys are deferred until the transaction's final state.
    for order in working.orders.values():
        if order is None:
            continue
        if working.customer_name(order[0]) is None:
            raise ValueError('transaction leaves orphan order')
    for key, name in working.customers.items():
        if name is None and _has_untouched_reference(connection, key, working.orders):
            raise ValueError('transaction leaves orphan order')

    # Writes are keyed and limited to touched rows: create/update customers,
    # then insert/update/delete orders, then delete customers.  Deleting
    # customers last keeps the flushed state referentially sane at each step.
    for key, name in working.customers.items():
        if name is not None:
            connection.execute(
                'INSERT INTO customers(key,name) VALUES(?,?) '
                'ON CONFLICT(key) DO UPDATE SET name=excluded.name', (key, name))
    for key, order in working.orders.items():
        if order is None:
            connection.execute('DELETE FROM orders WHERE key=?', (key,))
        else:
            connection.execute(
                'INSERT INTO orders(key,customer_id,amount_cents) VALUES(?,?,?) '
                'ON CONFLICT(key) DO UPDATE SET '
                'customer_id=excluded.customer_id,amount_cents=excluded.amount_cents',
                (key, order[0], order[1]))
    for key, name in working.customers.items():
        if name is None:
            connection.execute('DELETE FROM customers WHERE key=?', (key,))


def ingest(db_path, batch, before_commit=None, after_commit=None, trace=None):
    """Durably apply one ``{transactions,watermark}`` batch.

    The optional commit callbacks exist for tests; they run inside the
    database transaction just before COMMIT and immediately after it.  The
    optional ``trace`` callback is passed to ``sqlite3`` for diagnostics.
    """
    require(batch, 'transactions watermark')
    connection = _connect(db_path)
    if trace is not None:
        connection.set_trace_callback(trace)
    try:
        connection.execute('BEGIN IMMEDIATE')
        try:
            progress = _progress(connection)
            if progress['schema_version'] == LEGACY_SCHEMA_VERSION:
                _migrate_v1(connection)
                progress['schema_version'] = SCHEMA_VERSION
            base_lsn = progress['baseline_lsn']; old_watermark = progress['watermark']
            watermark = number(batch['watermark'])
            if watermark < old_watermark:
                raise ValueError('watermark must not decrease')

            incoming = []; seen_ids = {}; seen_lsns = {}; duplicates = 0
            for transaction in rows(batch['transactions'], 2000):
                tid, lsn, content = _transaction_record(transaction, base_lsn)
                if tid in seen_ids:
                    if seen_ids[tid] != content:
                        raise ValueError('conflicting transaction identity or commit LSN')
                    duplicates += 1; continue
                if lsn in seen_lsns:
                    raise ValueError('conflicting transaction identity or commit LSN')
                seen_ids[tid] = content; seen_lsns[lsn] = content
                incoming.append((tid, lsn, content))

            for tid, lsn, content in incoming:
                existing = connection.execute(
                    'SELECT id,commit_lsn,content FROM journal WHERE id=? OR commit_lsn=?',
                    (tid, lsn)).fetchall()
                if existing:
                    if all(row == (tid, lsn, content) for row in existing):
                        duplicates += 1; continue
                    raise ValueError('conflicting transaction identity or commit LSN')
                if lsn <= old_watermark:
                    raise ValueError('unknown transaction at or below the existing watermark')
                connection.execute('INSERT INTO journal(id,commit_lsn,content,applied) VALUES(?,?,?,0)',
                                   (tid, lsn, content))

            applied = []; last_applied = progress['last_applied_lsn']
            # Consume pending content one transaction at a time so the working
            # set never holds more than the current transaction's rows.  Keyset
            # paging keeps the candidate list bounded and avoids mutating the
            # journal while a scan over ``applied`` is still open.
            cursor = (0, '')
            while True:
                page = connection.execute(
                    'SELECT id,commit_lsn FROM journal WHERE applied=0 AND commit_lsn<=? '
                    'AND (commit_lsn,id)>(?,?) ORDER BY commit_lsn,id LIMIT 256',
                    (watermark, cursor[0], cursor[1])).fetchall()
                if not page:
                    break
                for tid, lsn in page:
                    content = connection.execute(
                        'SELECT content FROM journal WHERE id=?', (tid,)).fetchone()[0]
                    _apply_operations(connection, json.loads(content)['operations'])
                    connection.execute('UPDATE journal SET applied=1 WHERE id=?', (tid,))
                    applied.append({'id': tid, 'commit_lsn': lsn})
                    last_applied = max(last_applied, lsn)
                cursor = (page[-1][1], page[-1][0])

            connection.execute("UPDATE progress SET value=? WHERE key='watermark'", (watermark,))
            connection.execute("UPDATE progress SET value=? WHERE key='last_applied_lsn'",
                               (last_applied,))

            if before_commit is not None:
                before_commit()
            connection.execute('COMMIT')
        except BaseException:
            try:
                connection.execute('ROLLBACK')
            except sqlite3.Error:
                pass
            raise
        if after_commit is not None:
            after_commit()
        remaining = connection.execute(
            'SELECT id,commit_lsn FROM journal WHERE applied=0 ORDER BY commit_lsn,id').fetchall()
        return {'watermark': watermark, 'last_applied_lsn': last_applied,
                'applied_transactions': applied,
                'pending_transactions': [{'id': tid, 'commit_lsn': lsn} for tid, lsn in remaining],
                'duplicate_transactions': duplicates}
    finally:
        connection.close()


def export(db_path):
    """Read a consistent snapshot of tables, totals, progress and pending log."""
    connection = _connect(db_path)
    try:
        connection.execute('BEGIN')
        try:
            progress = _progress(connection)
            state = _read_state(connection, progress['schema_version'])
            pending_rows = connection.execute(
                'SELECT id,commit_lsn,content FROM journal WHERE applied=0 '
                'ORDER BY commit_lsn,id').fetchall()
            connection.execute('COMMIT')
        except BaseException:
            connection.execute('ROLLBACK'); raise
    finally:
        connection.close()
    totals = {key: 0 for key in state['customers']}
    for value in state['orders'].values():
        totals[value['customer_id']] += value['amount_cents']
    pending = [{'id': tid, 'commit_lsn': lsn, 'operations': len(json.loads(content)['operations'])}
               for tid, lsn, content in pending_rows]
    return {'schema_version': progress['schema_version'], 'watermark': progress['watermark'],
            'last_applied_lsn': progress['last_applied_lsn'], 'state': state,
            'customer_order_cents': totals, 'pending_transactions': pending}
