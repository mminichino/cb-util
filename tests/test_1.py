#!/usr/bin/env python3

import warnings
import pytest
import json
import string
import time
from couchbase.exceptions import (BucketNotFoundException, ScopeNotFoundException, CollectionNotFoundException)
from cbcmgr.cb_connect import CBConnect
from cbcmgr.cb_management import CBManager
from cbcmgr.cb_bucket import Bucket
from cbcmgr.config import UpsertMapConfig, MapUpsertType
from cbcmgr.cb_operation_s import CBOperation, Operation
from cbcmgr.cb_pathmap import CBPathMap
from cbcmgr.mt_pool import CBPool


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


@pytest.mark.parametrize("scope, collection", [("_default", "_default"), ("test", "test")])
@pytest.mark.parametrize("tls", [False, True])
def test_cb_driver_1(hostname, bucket, tls, scope, collection):
    replica_count = 0
    bucket_opts = Bucket(**dict(
        name=bucket,
        num_replicas=0
    ))

    dbm = CBManager(hostname, "Administrator", "password", ssl=False).connect()
    dbm.create_bucket(bucket_opts)
    dbm.create_scope(scope)
    dbm.create_collection(collection)
    result = dbm.get_bucket(bucket)
    assert result is not None

    dbc = CBConnect(hostname, "Administrator", "password", ssl=False).connect(bucket, scope, collection)

    dbm.cb_create_primary_index(replica=replica_count)
    index_name = dbm.cb_create_index(fields=["data"], replica=replica_count)
    dbm.index_wait()
    dbm.index_wait(index_name)
    result = dbm.is_index()
    assert result is True
    result = dbm.is_index(index_name)
    assert result is True
    dbc.cb_upsert("test::1", document)
    dbc.bucket_wait(bucket, count=1)
    result = dbc.cb_doc_exists("test::1")
    assert result is True

    result = dbc.has_primary_index()
    assert result is True
    result = dbc.cb_get("test::1")
    assert result == document
    result = dbc.collection_count(expect_count=1)
    assert result == 1
    result = dbc.cb_query(field="data", empty_retry=True)
    assert result == query_result
    dbc.cb_upsert("test::2", document)
    dbc.cb_subdoc_multi_upsert(["test::1", "test::2"], "data", ["new", "new"])
    result = dbc.cb_get("test::1")
    assert result == new_document
    result = dbc.collection_count(expect_count=2)
    assert result == 2
    dbc.cb_upsert("test::3", document)
    dbc.cb_subdoc_upsert("test::3", "data", "new")
    result = dbc.cb_get("test::3")
    assert result == new_document

    inventory = dbm.cluster_schema_dump()
    assert type(inventory) is dict

    dbm.cb_drop_primary_index()
    dbm.cb_drop_index(index_name)
    dbm.delete_wait()
    dbm.delete_wait(index_name)
    dbm.drop_bucket(bucket)


@pytest.mark.parametrize("scope, collection", [("_default", "_default"), ("test", "test")])
@pytest.mark.parametrize("tls", [False, True])
def test_cb_driver_2(hostname, bucket, tls, scope, collection):
    cfg = UpsertMapConfig().new()
    cfg.add('addresses.billing')
    cfg.add('addresses.delivery')
    cfg.add('history.events',
            p_type=MapUpsertType.LIST,
            id_key="event_id")

    p_map = CBPathMap(cfg, hostname, "Administrator", "password", bucket, scope, ssl=False, quota=128)
    p_map.connect()
    p_map.load_data("testdata", json_data=json.dumps(json_data, indent=2))
    CBOperation(hostname, "Administrator", "password", ssl=tls).connect(bucket).cleanup()


@pytest.mark.parametrize("scope, collection", [("_default", "_default"), ("test", "test")])
@pytest.mark.parametrize("tls", [False, True])
def test_cb_driver_3(hostname, bucket, tls, scope, collection):
    cfg = UpsertMapConfig().new()
    cfg.add('root.addresses.billing')
    cfg.add('root.addresses.delivery')
    cfg.add('root.history.events',
            p_type=MapUpsertType.LIST,
            id_key="event_id")

    p_map = CBPathMap(cfg, hostname, "Administrator", "password", bucket, scope, ssl=False, quota=128)
    p_map.connect()
    p_map.load_data("testdata", xml_data=xml_data)
    CBOperation(hostname, "Administrator", "password", ssl=tls).connect(bucket).cleanup()


@pytest.mark.parametrize("scope, collection", [("_default", "_default"), ("test", "test")])
@pytest.mark.parametrize("tls", [False, True])
def test_cb_driver_4(hostname, bucket, tls, scope, collection):
    keyspace = f"{bucket}.{scope}.{collection}"
    try:
        opm = CBOperation(hostname, "Administrator", "password", ssl=tls)
        col_a = opm.connect(keyspace)
        col_a.cleanup()
    except (BucketNotFoundException, ScopeNotFoundException, CollectionNotFoundException):
        pass

    col_a = CBOperation(hostname, "Administrator", "password", ssl=tls, quota=128, create=True).connect(keyspace)

    col_a.put_doc(col_a.collection, "test::1", document)
    d = col_a.get_doc(col_a.collection, "test::1")
    assert d == document

    col_a.index_by_query(f"select data from {keyspace}")

    r = col_a.run_query(col_a.cluster, f"select data from {keyspace}")
    assert r[0]['data'] == 'data'

    col_a.cleanup()

    col_t = CBOperation(hostname, "Administrator", "password", ssl=tls, quota=128, create=True).connect(keyspace)
    a_read = col_t.get_operator(Operation.READ)
    a_write = col_t.get_operator(Operation.WRITE)
    a_query = col_t.get_operator(Operation.QUERY)

    a_write.prep("test::1", document)
    a_write.execute()
    a_read.prep("test::1")
    a_read.execute()
    assert document == a_read.result["test::1"]

    col_t.index_by_query(f"select data from {keyspace}")
    a_query.prep(f"select data from {keyspace}")
    a_query.execute()
    assert a_query.result[0]['data'] == 'data'

    col_a.cleanup()


@pytest.mark.parametrize("scope", ["_default", "test"])
@pytest.mark.parametrize("collection", ["test"])
@pytest.mark.parametrize("tls", [False, True])
def test_cb_driver_5(hostname, bucket, tls, scope, collection):
    pool = CBPool(hostname, "Administrator", "password", ssl=tls, quota=128, create=True)

    for n in range(10):
        c = string.ascii_lowercase[n:n + 1]
        keyspace = f"{bucket}.{scope}.{collection}{c}"
        pool.connect(keyspace)
        for i in range(1000):
            pool.dispatch(keyspace, Operation.WRITE, f"test::{i+1}", document)

    pool.join()
    time.sleep(1)
    count = 0
    for n in range(10):
        c = string.ascii_lowercase[n:n + 1]
        keyspace = f"{bucket}.{scope}.{collection}{c}"
        count += CBOperation(hostname, "Administrator", "password", ssl=tls).connect(keyspace).get_count()
    assert count == 10000

    CBOperation(hostname, "Administrator", "password", ssl=tls).connect(bucket).cleanup()
