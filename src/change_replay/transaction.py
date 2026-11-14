"""A transaction's operations are applied in order to a private working copy."""
from copy import deepcopy
from .common import require,ident,number,rows

def customer(value):
    require(value,'name');ident(value['name']);return dict(value)

def order(value):
    require(value,'customer_id amount_cents');ident(value['customer_id']);number(value['amount_cents']);return dict(value)

VALIDATE={'customers':customer,'orders':order}

def apply(state,operations):
    staged=deepcopy(state)
    for op in rows(operations,1000):
        require(op,'table key before after');table=op['table'];key=ident(op['key'])
        if not isinstance(table,str) or table not in VALIDATE:raise ValueError('unknown change table')
        before=op['before'];after=op['after']
        if before is None and after is None:raise ValueError('empty change operation')
        if before is not None:VALIDATE[table](before)
        if after is not None:VALIDATE[table](after)
        if staged[table].get(key)!=before:raise ValueError('before image does not match current row')
        if after is None:del staged[table][key]
        else:staged[table][key]=dict(after)
    # Foreign keys are deferred until the transaction's final state.
    for value in staged['orders'].values():
        if value['customer_id'] not in staged['customers']:raise ValueError('transaction leaves orphan order')
    return staged
