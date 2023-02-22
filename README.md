

Couchbase Connection Manager
============================
Couchbase connection manager. Simplifies connecting to a Couchbase cluster and performing data and management operations.

Installing
==========
```
$ pip install cbcmgr
```

Usage
=====
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
