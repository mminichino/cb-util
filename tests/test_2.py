#!/usr/bin/env python3

import warnings
import pytest
import asyncio
import string
from couchbase.exceptions import (BucketNotFoundException, ScopeNotFoundException, CollectionNotFoundException)
from cbcmgr.cb_connect_lite_a import CBConnectLiteAsync
from cbcmgr.cb_operation_a import CBOperationAsync, Operation
from cbcmgr.async_pool import CBPoolAsync


warnings.filterwarnings("ignore")
document = {
    "id": 1,
    "data": "data",
    "one": "one",
    "two": "two",
    "three": "tree"
}
new_document = {
    "id": 1,
    "data": "new",
    "one": "one",
    "two": "two",
    "three": "tree"
}
query_result = [
    {
        'data': 'data'
    }
]

json_data = {
            "name": "John Doe",
            "email": "jdoe@example.com",
            "addresses": {
                "billing": {
                    "line1": "123 Any Street",
                    "line2": "Anywhere",
                    "country": "United States"
                },
                "delivery": {
                    "line1": "123 Any Street",
                    "line2": "Anywhere",
                    "country": "United States"
                }
            },
            "history": {
                "events": [
                    {
                        "event_id": "1",
                        "date": "1/1/1970",
                        "type": "contact"
                    },
                    {
                        "event_id": "2",
                        "date": "1/1/1970",
                        "type": "contact"
                    }
                ]
            },
            "purchases": {
                "complete": [
                    339, 976, 442, 777
                ],
                "abandoned": [
                    157, 42, 999
                ]
            }
        }

xml_data = """<?xml version="1.0" encoding="UTF-8" ?>
<root>
  <name>John Doe</name>
  <email>jdoe@example.com</email>
  <addresses>
    <billing>
      <line1>123 Any Street</line1>
      <line2>Anywhere</line2>
      <country>United States</country>
    </billing>
    <delivery>
      <line1>123 Any Street</line1>
      <line2>Anywhere</line2>
      <country>United States</country>
    </delivery>
  </addresses>
  <history>
    <events>
      <event_id>1</event_id>
      <date>1/1/1970</date>
      <type>contact</type>
    </events>
    <events>
      <event_id>2</event_id>
      <date>1/1/1970</date>
      <type>contact</type>
    </events>
  </history>
  <purchases>
    <complete>339</complete>
    <complete>976</complete>
    <complete>442</complete>
    <complete>777</complete>
    <abandoned>157</abandoned>
    <abandoned>42</abandoned>
    <abandoned>999</abandoned>
  </purchases>
</root>
"""


@pytest.mark.parametrize("bucket_name", ["test"])
@pytest.mark.parametrize("scope_name, collection_name", [("_default", "_default"), ("test", "test")])
@pytest.mark.parametrize("tls", [False, True])
@pytest.mark.asyncio
async def test_async_driver_1(hostname, bucket_name, tls, scope_name, collection_name):
    replica_count = 0
    keyspace = f"{bucket_name}.{scope_name}.{collection_name}"

    ca = CBConnectLiteAsync(hostname, "Administrator", "password", ssl=tls)
    cluster = await ca.session_a()

    await ca.create_bucket(cluster, bucket_name, quota=128)
    bucket = await ca.get_bucket(cluster, bucket_name)
    await ca.create_scope(bucket, scope_name)
    scope = await ca.get_scope(bucket, scope_name)
    await ca.create_collection(bucket, scope, collection_name)
    collection = await ca.get_collection(bucket, scope, collection_name)

    await ca.create_primary_index(cluster, bucket, scope, collection, replica=replica_count)
    await ca.create_indexes(cluster, bucket, scope, collection, fields=["data"], replica=replica_count)

    await ca.put_doc(collection, "test::1", document)
    result = await ca.get_doc(collection, "test::1")
    assert result == document

    result = await ca.collection_count(cluster, keyspace)
    assert result == 1

    result = await ca.run_query(cluster, f"select data from {keyspace}")
    assert result[0]['data'] == 'data'

    bm = cluster.buckets()
    await bm.drop_bucket(bucket_name)


@pytest.mark.parametrize("scope, collection", [("_default", "_default"), ("test", "test")])
@pytest.mark.parametrize("tls", [False, True])
@pytest.mark.asyncio
async def test_async_driver_2(hostname, bucket, tls, scope, collection):
    keyspace = f"{bucket}.{scope}.{collection}"
    try:
        opc = CBOperationAsync(hostname, "Administrator", "password", ssl=tls)
        opm = await opc.init()
        col_a = await opm.connect(keyspace)
        col_a.cleanup()
    except (BucketNotFoundException, ScopeNotFoundException, CollectionNotFoundException):
        pass

    opc = CBOperationAsync(hostname, "Administrator", "password", ssl=tls, quota=128, create=True)
    opm = await opc.init()
    col_a = await opm.connect(keyspace)

    await col_a.put_doc(col_a.collection, "test::1", document)
    d = await col_a.get_doc(col_a.collection, "test::1")
    assert d == document

    await col_a.index_by_query(f"select data from {keyspace}")

    r = await col_a.run_query(col_a.cluster, f"select data from {keyspace}")
    assert r[0]['data'] == 'data'

    await col_a.cleanup()

    opc = CBOperationAsync(hostname, "Administrator", "password", ssl=tls, quota=128, create=True)
    opm = await opc.init()
    col_t = await opm.connect(keyspace)
    a_read = col_t.get_operator(Operation.READ)
    a_write = col_t.get_operator(Operation.WRITE)
    a_query = col_t.get_operator(Operation.QUERY)

    a_write.prep("test::1", document)
    await a_write.execute()
    a_read.prep("test::1")
    await a_read.execute()
    assert document == a_read.result["test::1"]

    await col_t.index_by_query(f"select data from {keyspace}")
    a_query.prep(f"select data from {keyspace}")
    await a_query.execute()
    assert a_query.result[0]['data'] == 'data'

    await col_a.cleanup()


@pytest.mark.parametrize("scope", ["_default", "test"])
@pytest.mark.parametrize("collection", ["test"])
@pytest.mark.parametrize("tls", [False, True])
@pytest.mark.asyncio
async def test_async_driver_3(hostname, bucket, tls, scope, collection):
    pool = CBPoolAsync(hostname, "Administrator", "password", ssl=False, quota=128, create=True)

    for n in range(10):
        c = string.ascii_lowercase[n:n + 1]
        keyspace = f"{bucket}.{scope}.{collection}{c}"
        await pool.connect(keyspace)
        for i in range(1000):
            await pool.dispatch(keyspace, Operation.WRITE, f"test::{i + 1}", document)

    await pool.join()
    await asyncio.sleep(1)
    count = 0
    for n in range(10):
        c = string.ascii_lowercase[n:n + 1]
        keyspace = f"{bucket}.{scope}.{collection}{c}"
        opc = CBOperationAsync(hostname, "Administrator", "password", ssl=tls)
        opm = await opc.init()
        opk = await opm.connect(keyspace)
        count += await opk.get_count()
    assert count == 10000
