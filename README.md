# cb-util 2.1.1

## Couchbase Utilities
Couchbase connection manager. Simplifies connecting to a Couchbase cluster and performing data and management operations.

## Installing
```
$ pip install cbcmgr
```

## Usage
Original syntax (package is backwards compatible):
```
>>> from cbcmgr.cb_connect import CBConnect
>>> from cbcmgr.cb_management import CBManager
>>> bucket = scope = collection = "test"
>>> dbm = CBManager("127.0.0.1", "Administrator", "password", ssl=False).connect()
>>> dbm.create_bucket(bucket)
>>> dbm.create_scope(scope)
>>> dbm.create_collection(collection)
>>> dbc = CBConnect("127.0.0.1", "Administrator", "password", ssl=False).connect(bucket, scope, collection)
>>> result = dbc.cb_upsert("test::1", {"data": 1})
>>> result = dbc.cb_get("test::1")
>>> print(result)
{'data': 1}
```
New Operator syntax:
```
keyspace = "test.test.test"
db = CBOperation(hostname, "Administrator", "password", ssl=False, quota=128, create=True).connect(keyspace)
db.put_doc(col_a.collection, "test::1", document)
d = db.get_doc(col_a.collection, "test::1")
assert d == document
db.index_by_query("select data from test.test.test")
r = db.run_query(col_a.cluster, "select data from test.test.test")
assert r[0]['data'] == 'data'
```
Thread Pool Syntax:
```
pool = CBPool(hostname, "Administrator", "password", ssl=False, quota=128, create=True)
pool.connect(keyspace)
pool.dispatch(keyspace, Operation.WRITE, f"test::1", document)
pool.join()
```
Async Pool Syntax
```
pool = CBPoolAsync(hostname, "Administrator", "password", ssl=False, quota=128, create=True)
await pool.connect(keyspace)
await pool.join()
```
