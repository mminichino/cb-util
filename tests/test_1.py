#!/usr/bin/env python3

import warnings
import pytest
import json
from cbcmgr.cb_connect import CBConnect
from cbcmgr.cb_management import CBManager
from cbcmgr.config import UpsertMapConfig, MapUpsertType, KeyStyle


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

    dbm = CBManager(hostname, "Administrator", "password", ssl=False).connect()
    dbm.create_bucket(bucket)
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
    dbm = CBManager(hostname, "Administrator", "password", ssl=False).connect()
    dbm.create_bucket(bucket)
    dbm.create_scope(scope)
    dbm.create_collection(collection)

    cfg = UpsertMapConfig().new()
    cfg.add('addresses.billing', collection=True)
    cfg.add('addresses.delivery', collection=True)
    cfg.add('history.events',
            p_type=MapUpsertType.LIST,
            collection=True,
            doc_id=KeyStyle.TEXT_FIELD,
            id_key="event_id")

    dbm.cb_map_upsert("testdata", cfg, json_data=json.dumps(json_data, indent=2))


@pytest.mark.parametrize("scope, collection", [("_default", "_default"), ("test", "test")])
@pytest.mark.parametrize("tls", [False, True])
def test_cb_driver_3(hostname, bucket, tls, scope, collection):
    dbm = CBManager(hostname, "Administrator", "password", ssl=False).connect()
    dbm.create_bucket(bucket)
    dbm.create_scope(scope)
    dbm.create_collection(collection)

    cfg = UpsertMapConfig().new()
    cfg.add('root.addresses.billing', collection=True)
    cfg.add('root.addresses.delivery', collection=True)
    cfg.add('root.history.events',
            p_type=MapUpsertType.LIST,
            collection=True,
            doc_id=KeyStyle.TEXT_FIELD,
            id_key="event_id")

    dbm.cb_map_upsert("testdata", cfg, xml_data=xml_data)
