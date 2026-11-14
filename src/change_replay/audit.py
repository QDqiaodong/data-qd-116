"""Read-only comparison of a durable database against an NDJSON reference.

The reference is a byte stream, not a JSON document: its first line is
``{"watermark": N}`` and every following line is
``{"table": ..., "key": ..., "value": ...}``.  Rows may appear in any order,
so the reference is normalised into a temporary SQLite file (temporary disk,
bounded memory) instead of being loaded into a list.  Duplicate keys (per
table), duplicate JSON fields, malformed lines (including a bad final line)
and orphan orders are all rejected before anything is published; every
rejection names its line.

The database is opened read-only and read inside one deferred transaction.
Progress and the two materialised tables therefore come from one snapshot;
under WAL a concurrent ``ingest`` may commit while the audit is running
without the report mixing two versions.  A v1 database is read with its
legacy JSON rows and is never migrated or rewritten.

The final report is published atomically by the caller only after the whole
file validated and compared.  Differences are a normal result (exit 0); bad
input or a watermark mismatch is a validation error (exit 2).
"""
from pathlib import Path
import hashlib, json, os, sqlite3, tempfile

from .common import ident, number, require
from .store import LEGACY_SCHEMA_VERSION, SCHEMA_VERSION, _canonical
from .transaction import VALIDATE

SAMPLE_LIMIT = 20
MAX_LINE_BYTES = 1_000_000
TABLES = ('customers', 'orders')
CATEGORIES = ('db_only', 'reference_only', 'value_differs')
_PROGRESS_KEYS = ('schema_version', 'baseline_lsn', 'watermark', 'last_applied_lsn')

_REFERENCE_SCHEMA = (
    'CREATE TABLE reference('
    'tbl TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, customer_id TEXT, '
    'PRIMARY KEY(tbl,key)) WITHOUT ROWID'
)


# --------------------------------------------------------------------------- #
# Strict NDJSON parsing


def _unique(pairs):
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError('duplicate JSON key: ' + name)
        result[name] = value
    return result


def _constant(value):
    raise ValueError('nonfinite JSON number: ' + value)


def _strict(text):
    return json.loads(text, object_pairs_hook=_unique, parse_constant=_constant)


def _bad_line(lineno, detail):
    return ValueError(f'bad reference line {lineno}: {detail}')


def _line_object(raw, lineno):
    """Decode one byte line without ever materialising the whole file."""
    if len(raw) > MAX_LINE_BYTES:
        raise _bad_line(lineno, 'line exceeds 1 MB')
    try:
        text = raw.decode('utf-8')
    except UnicodeDecodeError:
        raise _bad_line(lineno, 'invalid UTF-8') from None
    try:
        return _strict(text)
    except ValueError as error:
        raise _bad_line(lineno, str(error)) from None


def _line_row(connection, obj, lineno):
    try:
        require(obj, 'table key value')
        table = obj['table']
        key = ident(obj['key'])
        if not isinstance(table, str) or table not in VALIDATE:
            raise ValueError('unknown reference table')
        value = VALIDATE[table](obj['value'])
        customer_id = value['customer_id'] if table == 'orders' else None
        connection.execute(
            'INSERT INTO reference(tbl,key,value,customer_id) VALUES(?,?,?,?)',
            (table, key, _canonical(value), customer_id))
    except sqlite3.IntegrityError:
        raise _bad_line(lineno, 'duplicate reference key') from None
    except ValueError as error:
        raise _bad_line(lineno, str(error)) from None


def _load_reference(connection, source):
    """Stream the whole file; return (sha256 hex, watermark)."""
    digest = hashlib.sha256()
    watermark = None
    try:
        stream = Path(source).open('rb')
    except OSError as error:
        raise ValueError(f'cannot read reference: {error}') from None
    lineno = 0
    with stream:
        try:
            while True:
                raw = stream.readline(MAX_LINE_BYTES + 2)
                if not raw:
                    break
                lineno += 1
                digest.update(raw)
                if len(raw) > MAX_LINE_BYTES + 1 or (
                        not raw.endswith(b'\n') and len(raw) > MAX_LINE_BYTES):
                    raise _bad_line(lineno, 'line exceeds 1 MB')
                if raw.endswith(b'\n'):
                    raw = raw[:-1]
                obj = _line_object(raw, lineno)
                if watermark is None:
                    try:
                        require(obj, 'watermark')
                        watermark = number(obj['watermark'])
                    except ValueError as error:
                        raise _bad_line(lineno, str(error)) from None
                    continue
                _line_row(connection, obj, lineno)
        except OSError as error:
            raise ValueError(f'cannot read reference line {lineno + 1}: {error}') from None
    if watermark is None:
        raise ValueError('empty reference')
    return digest.hexdigest(), watermark


def _reject_orphans(connection):
    row = connection.execute(
        "SELECT o.key FROM reference AS o WHERE o.tbl='orders' AND NOT EXISTS ("
        "SELECT 1 FROM reference AS c WHERE c.tbl='customers' AND c.key=o.customer_id) "
        'LIMIT 1').fetchone()
    if row is not None:
        raise ValueError('reference leaves orphan order')


class _Reference:
    """Validated reference rows in a temporary SQLite database."""

    def __init__(self, source):
        handle, temp = tempfile.mkstemp(prefix='.change-replay-audit-', suffix='.db')
        os.close(handle)
        self.path = temp
        self.connection = None
        try:
            self.connection = sqlite3.connect(temp, isolation_level=None)
            self.connection.execute('PRAGMA journal_mode=OFF')
            self.connection.execute('PRAGMA synchronous=OFF')
            self.connection.execute(_REFERENCE_SCHEMA)
            self.connection.execute('BEGIN')
            self.digest, self.watermark = _load_reference(self.connection, source)
            _reject_orphans(self.connection)
            self.connection.execute('COMMIT')
        except BaseException:
            self.close()
            raise

    def close(self):
        if self.connection is not None:
            try:
                self.connection.close()
            except sqlite3.Error:
                pass
            self.connection = None
        try:
            os.unlink(self.path)
        except OSError:
            pass


# --------------------------------------------------------------------------- #
# Database snapshot and streaming comparison


def _open_snapshot(db_path):
    """Open the database read-only and start the audit's read transaction.

    ``mode=ro`` is enforced by SQLite itself, so the audit cannot migrate a v1
    database, checkpoint WAL or rewrite anything; ``query_only`` additionally
    blocks accidental writes on the Python side.
    """
    path = Path(db_path)
    if not path.exists():
        raise ValueError('database is not initialized')
    connection = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True,
                                 isolation_level=None)
    try:
        connection.execute('PRAGMA query_only=ON')
        connection.execute('BEGIN')
        try:
            progress = dict(connection.execute('SELECT key,value FROM progress'))
        except sqlite3.DatabaseError as error:
            if 'no such table' in str(error) or 'not a database' in str(error):
                raise ValueError('database is not initialized') from None
            raise
        if progress.get('schema_version') not in (LEGACY_SCHEMA_VERSION, SCHEMA_VERSION):
            raise ValueError('unsupported database schema')
        for key in _PROGRESS_KEYS:
            if key == 'schema_version':
                continue
            try:
                number(progress.get(key))
            except ValueError:
                raise ValueError('database progress is incomplete') from None
        return connection, progress
    except BaseException:
        connection.close()
        raise


def _db_rows(connection, schema_version, table):
    """Yield (key, canonical value text, value object) sorted by key."""
    if schema_version == LEGACY_SCHEMA_VERSION:
        sql = 'SELECT key,value FROM {} ORDER BY key'.format(table)
        for key, raw in connection.execute(sql):
            try:
                value = VALIDATE[table](_strict(raw))
            except ValueError as error:
                raise ValueError(f'invalid database {table} row: {error}') from None
            yield key, _canonical(value), value
    elif table == 'customers':
        cursor = connection.execute('SELECT key,name FROM customers ORDER BY key')
        for key, name in cursor:
            try:
                value = VALIDATE[table]({'name': name})
            except ValueError as error:
                raise ValueError(f'invalid database {table} row: {error}') from None
            yield key, _canonical(value), value
    else:
        cursor = connection.execute(
            'SELECT key,customer_id,amount_cents FROM orders ORDER BY key')
        for key, customer_id, amount in cursor:
            try:
                value = VALIDATE[table]({'customer_id': customer_id,
                                         'amount_cents': amount})
            except ValueError as error:
                raise ValueError(f'invalid database {table} row: {error}') from None
            yield key, _canonical(value), value


def _note(counts, samples, category, key, db_value, reference_value):
    counts[category] += 1
    if len(samples[category]) < SAMPLE_LIMIT:
        samples[category].append({'key': key, 'db': db_value,
                                  'reference': reference_value})


def _compare(connection, schema_version, reference, table):
    """Merge-join the sorted database and reference rows for one table."""
    counts = {category: 0 for category in CATEGORIES}
    samples = {category: [] for category in CATEGORIES}
    db_rows = _db_rows(connection, schema_version, table)
    reference_rows = reference.connection.execute(
        'SELECT key,value FROM reference WHERE tbl=? ORDER BY key', (table,))
    db_row = next(db_rows, None)
    ref_row = next(reference_rows, None)
    while db_row is not None or ref_row is not None:
        if ref_row is None or (db_row is not None and db_row[0] < ref_row[0]):
            key, _, value = db_row
            _note(counts, samples, 'db_only', key, value, None)
            db_row = next(db_rows, None)
        elif db_row is None or ref_row[0] < db_row[0]:
            key, text = ref_row
            _note(counts, samples, 'reference_only', key, None, _strict(text))
            ref_row = next(reference_rows, None)
        else:
            key, text, value = db_row
            if text != ref_row[1]:
                _note(counts, samples, 'value_differs', key, value,
                      _strict(ref_row[1]))
            db_row = next(db_rows, None)
            ref_row = next(reference_rows, None)
    return {category: {'count': counts[category], 'samples': samples[category]}
            for category in CATEGORIES}


def run(db_path, reference_path, after_begin=None):
    """Audit one database against one reference; return the report object.

    ``after_begin`` is a test hook that runs after the database read
    transaction started and progress was read but before the tables are
    compared.  It lets a concurrent writer commit in between; the audit must
    still see one snapshot.
    """
    reference = _Reference(reference_path)
    try:
        connection, progress = _open_snapshot(db_path)
        try:
            if reference.watermark != progress['watermark']:
                raise ValueError('reference watermark does not match database watermark')
            if after_begin is not None:
                after_begin()
            tables = {table: _compare(connection, progress['schema_version'],
                                      reference, table) for table in TABLES}
            connection.execute('COMMIT')
        except BaseException:
            try:
                connection.execute('ROLLBACK')
            except sqlite3.Error:
                pass
            raise
        finally:
            connection.close()
    finally:
        reference.close()
    return {
        'schema_version': 1,
        'source_sha256': reference.digest,
        'progress': {key: progress[key] for key in _PROGRESS_KEYS},
        'reference_watermark': reference.watermark,
        'sample_limit': SAMPLE_LIMIT,
        'tables': tables,
    }
