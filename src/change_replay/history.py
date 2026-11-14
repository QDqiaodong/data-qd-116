"""SCD2 version history derived from committed transaction states only.

Each live row has exactly one open version. Versions change only at
transaction commit LSNs, by diffing the committed states before and after
a transaction: intra-transaction intermediate states never appear, a
transaction whose net effect on a key is zero does not split its version,
deletion closes the interval, recreating the same key opens a new segment,
and no tombstone rows are emitted for periods when a key does not exist.
"""
TABLES=('customers','orders')

def version(table,key,value,from_lsn):
    return {'table':table,'key':key,'value':dict(value),'from_lsn':from_lsn,'to_lsn':None}

class History:
    def __init__(self,base_lsn,state):
        self.open={};self.closed=[]
        for table in TABLES:
            for key,value in state[table].items():
                self.open[(table,key)]=version(table,key,value,base_lsn)
    def commit(self,lsn,before,after):
        for table in TABLES:
            old,new=before[table],after[table]
            for key,value in old.items():
                if key not in new or new[key]!=value:
                    current=self.open.pop((table,key));current['to_lsn']=lsn;self.closed.append(current)
            for key,value in new.items():
                if key not in old or old[key]!=value:
                    self.open[(table,key)]=version(table,key,value,lsn)
    def versions(self):
        return sorted([*self.closed,*self.open.values()],key=lambda v:(v['table'],v['key'],v['from_lsn']))
