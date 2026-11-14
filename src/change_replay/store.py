"""SQLite-backed durable receive pipeline.

A database is initialised once from an existing baseline, then fed batched
transaction logs.  Every batch commits its identity/pending journal, both
materialised tables and the progress counters in one SQLite transaction: a
crash before the commit can only leave the previous version, a crash after it
can only leave the new one.  Applied transaction identities stay in the
database, so a restart never replays them and never rereads an old source
file.

The watermark is a receive frontier, not application progress.  It means no
transaction with ``commit_lsn <= watermark`` will ever arrive for the first
time again; LSN holes are therefore permanent and ``last_applied_lsn`` may be
smaller than the watermark.  Transactions beyond the new watermark are kept
as pending after shape validation only; they are applied when a later batch
moves the watermark past their commit LSN.
"""
from pathlib import Path
import json, os, sqlite3, tempfile

from .common import ident, number, require, rows
from .transaction import VALIDATE, apply

SCHEMA_VERSION=1
TABLES=('customers','orders')

SCHEMA="""\
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

BASELINE_FIELDS='lsn customers orders'


def _canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'))


def _baseline(document):
    """Extract and validate the baseline from a baseline or legacy input doc."""
    if not isinstance(document,dict):raise ValueError('expected baseline document')
    if 'baseline' in document:
        # Also accept the legacy input document; only its baseline is used.
        if set(document) not in ({'baseline'},{'baseline','transactions','through_lsn'}):
            raise ValueError('unexpected baseline document fields')
        base=document['baseline']
    elif set(document)==set(BASELINE_FIELDS.split()):
        base=document
    else:
        raise ValueError('expected baseline object or input document')
    require(base,BASELINE_FIELDS)
    base_lsn=number(base['lsn'])
    state={'customers':{},'orders':{}}
    for table,validator in (('customers',VALIDATE['customers']),('orders',VALIDATE['orders'])):
        for row in rows(base[table]):
            require(row,'key value');key=ident(row['key'])
            if key in state[table]:raise ValueError('duplicate baseline key')
            state[table][key]=validator(row['value'])
    for value in state['orders'].values():
        if value['customer_id'] not in state['customers']:
            raise ValueError('baseline leaves orphan order')
    return base_lsn,state


def _sync(path):
    try:
        fd=os.open(path,os.O_RDONLY)
        try:os.fsync(fd)
        finally:os.close(fd)
    except OSError:pass


def _connect(db_path):
    path=Path(db_path)
    if not path.exists():raise ValueError('database is not initialized')
    connection=sqlite3.connect(path,isolation_level=None)
    connection.execute('PRAGMA synchronous=FULL')
    try:
        row=connection.execute("SELECT value FROM progress WHERE key='schema_version'").fetchone()
    except sqlite3.DatabaseError:
        connection.close();raise ValueError('database is not initialized') from None
    if row is None or row[0]!=SCHEMA_VERSION:
        connection.close();raise ValueError('unsupported database schema')
    return connection


def _progress(connection):
    return dict(connection.execute('SELECT key,value FROM progress'))


def _state(connection):
    state={'customers':{},'orders':{}}
    for table in TABLES:
        for key,value in connection.execute(f'SELECT key,value FROM {table} ORDER BY key'):
            state[table][key]=json.loads(value)
    return state


def _write_state(connection,state):
    for table in TABLES:
        connection.execute(f'DELETE FROM {table}')
        connection.executemany(f'INSERT INTO {table}(key,value) VALUES(?,?)',
                               [(key,_canonical(value)) for key,value in state[table].items()])


def initialize(db_path,document):
    """Create a new database from an existing baseline; never overwrite."""
    base_lsn,state=_baseline(document)
    path=Path(db_path)
    if path.is_symlink() or path.exists():raise ValueError('database path already exists')
    path.parent.mkdir(parents=True,exist_ok=True)
    handle,temp=tempfile.mkstemp(prefix='.change-replay-init-',suffix='.db',dir=path.parent)
    os.close(handle);os.unlink(temp)
    try:
        connection=sqlite3.connect(temp,isolation_level=None)
        try:
            connection.execute('PRAGMA synchronous=FULL')
            connection.executescript(SCHEMA)
            connection.execute('BEGIN IMMEDIATE')
            try:
                connection.executemany('INSERT INTO progress(key,value) VALUES(?,?)',
                                       [('schema_version',SCHEMA_VERSION),
                                        ('baseline_lsn',base_lsn),
                                        ('watermark',base_lsn),
                                        ('last_applied_lsn',base_lsn)])
                _write_state(connection,state)
                connection.execute('COMMIT')
            except BaseException:
                connection.execute('ROLLBACK');raise
        finally:
            connection.close()
        _sync(temp);os.replace(temp,path);_sync(path.parent)
    except BaseException:
        try:os.unlink(temp)
        except OSError:pass
        raise
    return {'baseline_lsn':base_lsn,'watermark':base_lsn,'last_applied_lsn':base_lsn,
            'customers':len(state['customers']),'orders':len(state['orders'])}


def _transaction_record(transaction,base_lsn):
    """Validate one transaction object; return (id,commit_lsn,canonical)."""
    require(transaction,'id commit_lsn operations')
    tid=ident(transaction['id'])
    lsn=number(transaction['commit_lsn'],base_lsn+1)
    operations=rows(transaction['operations'],1000)
    for operation in operations:
        require(operation,'table key before after');ident(operation['key'])
        table=operation['table']
        if not isinstance(table,str) or table not in VALIDATE:raise ValueError('unknown change table')
        before,after=operation['before'],operation['after']
        if before is None and after is None:raise ValueError('empty change operation')
        for value in (before,after):
            if value is not None:VALIDATE[table](value)
    return tid,lsn,_canonical(transaction)


def ingest(db_path,batch,before_commit=None,after_commit=None):
    """Durably apply one ``{transactions,watermark}`` batch.

    The optional callbacks exist for tests; they run inside the database
    transaction just before COMMIT and immediately after it.
    """
    require(batch,'transactions watermark')
    connection=_connect(db_path)
    try:
        connection.execute('BEGIN IMMEDIATE')
        try:
            progress=_progress(connection)
            base_lsn=progress['baseline_lsn'];old_watermark=progress['watermark']
            watermark=number(batch['watermark'])
            if watermark<old_watermark:raise ValueError('watermark must not decrease')

            incoming=[];seen_ids={};seen_lsns={};duplicates=0
            for transaction in rows(batch['transactions'],2000):
                tid,lsn,content=_transaction_record(transaction,base_lsn)
                if tid in seen_ids:
                    if seen_ids[tid]!=content:raise ValueError('conflicting transaction identity or commit LSN')
                    duplicates+=1;continue
                if lsn in seen_lsns:raise ValueError('conflicting transaction identity or commit LSN')
                seen_ids[tid]=content;seen_lsns[lsn]=content
                incoming.append((tid,lsn,content))

            for tid,lsn,content in incoming:
                existing=connection.execute(
                    'SELECT id,commit_lsn,content FROM journal WHERE id=? OR commit_lsn=?',
                    (tid,lsn)).fetchall()
                if existing:
                    if all(row==(tid,lsn,content) for row in existing):
                        duplicates+=1;continue
                    raise ValueError('conflicting transaction identity or commit LSN')
                if lsn<=old_watermark:
                    raise ValueError('unknown transaction at or below the existing watermark')
                connection.execute('INSERT INTO journal(id,commit_lsn,content,applied) VALUES(?,?,?,0)',
                                   (tid,lsn,content))

            state=_state(connection);applied=[];last_applied=progress['last_applied_lsn']
            pending=connection.execute(
                'SELECT id,commit_lsn,content FROM journal WHERE applied=0 AND commit_lsn<=? ORDER BY commit_lsn',
                (watermark,)).fetchall()
            for tid,lsn,content in pending:
                state=apply(state,json.loads(content)['operations'])
                connection.execute('UPDATE journal SET applied=1 WHERE id=?',(tid,))
                applied.append({'id':tid,'commit_lsn':lsn})
                last_applied=max(last_applied,lsn)

            _write_state(connection,state)
            connection.execute("UPDATE progress SET value=? WHERE key='watermark'",(watermark,))
            connection.execute("UPDATE progress SET value=? WHERE key='last_applied_lsn'",(last_applied,))

            if before_commit is not None:before_commit()
            connection.execute('COMMIT')
        except BaseException:
            try:connection.execute('ROLLBACK')
            except sqlite3.Error:pass
            raise
        if after_commit is not None:after_commit()
        remaining=connection.execute(
            'SELECT id,commit_lsn FROM journal WHERE applied=0 ORDER BY commit_lsn,id').fetchall()
        return {'watermark':watermark,'last_applied_lsn':last_applied,
                'applied_transactions':applied,
                'pending_transactions':[{'id':tid,'commit_lsn':lsn} for tid,lsn in remaining],
                'duplicate_transactions':duplicates}
    finally:
        connection.close()


def export(db_path):
    """Read a consistent snapshot of tables, totals, progress and pending log."""
    connection=_connect(db_path)
    try:
        connection.execute('BEGIN')
        try:
            progress=_progress(connection)
            state=_state(connection)
            pending_rows=connection.execute(
                'SELECT id,commit_lsn,content FROM journal WHERE applied=0 ORDER BY commit_lsn,id').fetchall()
            connection.execute('COMMIT')
        except BaseException:
            connection.execute('ROLLBACK');raise
    finally:
        connection.close()
    totals={key:0 for key in state['customers']}
    for value in state['orders'].values():totals[value['customer_id']]+=value['amount_cents']
    pending=[{'id':tid,'commit_lsn':lsn,'operations':len(json.loads(content)['operations'])}
             for tid,lsn,content in pending_rows]
    return {'schema_version':SCHEMA_VERSION,'watermark':progress['watermark'],
            'last_applied_lsn':progress['last_applied_lsn'],'state':state,
            'customer_order_cents':totals,'pending_transactions':pending}
