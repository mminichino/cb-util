

Couchbase Utility
=================
Couchbase connection manager. Simplifies connecting to a Couchbase cluster and performing data and management operations.

Installing
==========

.. code-block::

    $ pip install cbutil

Usage
=====

.. code-block::

    >>> from cbutil.cb_connect import CBConnect
    >>> db = CBConnect("127.0.0.1", "Administrator", "password", ssl=False).connect().create_bucket("test").create_scope("test").create_collection("test")
    >>> result = db.cb_upsert("test::1", {"data": 1})
    >>> result = db.cb_get("test::1")
    >>> print(result)
    {'data': 1}
