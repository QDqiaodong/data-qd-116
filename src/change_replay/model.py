import json,hashlib
from .common import require,ident,number,rows
from .transaction import apply,customer,order

def analyze(data):
    require(data,'baseline transactions through_lsn');base=data['baseline'];require(base,'lsn customers orders')
    base_lsn=number(base['lsn']);through=number(data['through_lsn'],base_lsn)
    state={'customers':{},'orders':{}}
    for table,validator in [('customers',customer),('orders',order)]:
        for row in rows(base[table]):
            require(row,'key value');key=ident(row['key'])
            if key in state[table]:raise ValueError('duplicate baseline key')
            state[table][key]=validator(row['value'])
    state=apply(state,[]);by_lsn={};by_id={};duplicates=0
    for tx in rows(data['transactions'],2000):
        require(tx,'id commit_lsn operations');tid=ident(tx['id']);lsn=number(tx['commit_lsn'],base_lsn+1)
        rows(tx['operations'],1000)
        canonical=json.dumps(tx,sort_keys=True,separators=(',',':'))
        if lsn in by_lsn or tid in by_id:
            if by_lsn.get(lsn)==canonical and by_id.get(tid)==canonical:duplicates+=1;continue
            raise ValueError('conflicting transaction identity or commit LSN')
        by_lsn[lsn]=canonical;by_id[tid]=canonical
    applied=[];future=[]
    for lsn,payload in sorted(by_lsn.items()):
        tx=json.loads(payload)
        if lsn>through:
            # Validate future shapes against the declared table contract without requiring current before images.
            for op in tx['operations']:
                require(op,'table key before after');ident(op['key'])
                if op['table'] not in ('customers','orders'):raise ValueError('unknown future table')
                validator=customer if op['table']=='customers' else order
                if op['before'] is None and op['after'] is None:raise ValueError('empty future operation')
                for value in (op['before'],op['after']):
                    if value is not None:validator(value)
            future.append(tx['id']);continue
        state=apply(state,tx['operations']);applied.append({'id':tx['id'],'commit_lsn':lsn,'operations':len(tx['operations'])})
    totals={key:0 for key in state['customers']}
    for value in state['orders'].values():totals[value['customer_id']]+=value['amount_cents']
    content=json.dumps(state,sort_keys=True,separators=(',',':')).encode()
    return {'requested_through_lsn':through,'last_applied_lsn':applied[-1]['commit_lsn'] if applied else base_lsn,
            'state':state,'customer_order_cents':totals,'applied_transactions':applied,'future_transactions':future,
            'duplicate_transactions':duplicates,'state_sha256':hashlib.sha256(content).hexdigest()}
