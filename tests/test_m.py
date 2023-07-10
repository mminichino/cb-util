#!/usr/bin/env python3

import warnings
import sys
import argparse
import os
import logging
import json
from couchbase.exceptions import (BucketNotFoundException, ScopeNotFoundException, CollectionNotFoundException)

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
sys.path.append(current)

from cbcmgr.cb_connect import CBConnect
from cbcmgr.cb_management import CBManager
from cbcmgr.cb_operation_s import CBOperation
from cbcmgr.config import UpsertMapConfig, MapUpsertType, KeyStyle
from conftest import pytest_sessionstart, pytest_sessionfinish

logger = logging.getLogger()

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


class Params(object):

    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--ssl', action='store_true', help="Use SSL")
        parser.add_argument('--host', action='store', help="Hostname or IP address", default="127.0.0.1")
        parser.add_argument('--user', action='store', help="User Name", default="Administrator")
        parser.add_argument('--password', action='store', help="User Password", default="password")
        parser.add_argument('--bucket', action='store', help="Test Bucket", default="testrun")
        parser.add_argument('--start', action='store_true', help="Start Container")
        parser.add_argument('--stop', action='store_true', help="Stop Container")
        parser.add_argument("--external", action="store_true")
        parser.add_argument("--pool", action="store_true")
        parser.add_argument("--verbose", action="store_true")
        parser.add_argument("--file", action="store", help="Input File")
        self.args = parser.parse_args()

    @property
    def parameters(self):
        return self.args


def container_start():
    pytest_sessionstart(None)


def container_stop():
    pytest_sessionfinish(None, 0)


def manual_1(hostname, bucket, tls, scope, collection):
    replica_count = 0

    print("=> Connect")
    dbm = CBManager(hostname, "Administrator", "password", ssl=False).connect()
    dbm.create_bucket(bucket, quota=100)
    dbm.create_scope(scope)
    dbm.create_collection(collection)
    dbc = CBConnect(hostname, "Administrator", "password", ssl=False).connect(bucket, scope, collection)
    print("=> Create indexes")
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
    print("=> Data tests")
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
    print("=> Cleanup")
    dbm.cb_drop_primary_index()
    dbm.cb_drop_index(index_name)
    dbm.delete_wait()
    dbm.delete_wait(index_name)
    dbm.drop_bucket(bucket)


def manual_2(hostname, bucket, tls, scope, collection):
    dbm = CBManager(hostname, "Administrator", "password", ssl=False).connect()
    dbm.create_bucket(bucket, quota=100)
    dbm.create_scope(scope)
    dbm.create_collection(collection)

    print("=> Map Test JSON")
    cfg = UpsertMapConfig().new()
    cfg.add('addresses.billing', collection=True)
    cfg.add('addresses.delivery', collection=True)
    cfg.add('history.events',
            p_type=MapUpsertType.LIST,
            collection=True,
            doc_id=KeyStyle.TEXT_FIELD,
            id_key="event_id")

    dbm.cb_map_upsert("testdata", cfg, json_data=json.dumps(json_data, indent=2))
    print("=> Cleanup")
    dbm.drop_bucket(bucket)


def manual_3(hostname, bucket, tls, scope, collection):
    dbm = CBManager(hostname, "Administrator", "password", ssl=False).connect()
    dbm.create_bucket(bucket, quota=100)
    dbm.create_scope(scope)
    dbm.create_collection(collection)

    print("=> Map Test XML")
    cfg = UpsertMapConfig().new()
    cfg.add('root.addresses.billing', collection=True)
    cfg.add('root.addresses.delivery', collection=True)
    cfg.add('root.history.events',
            p_type=MapUpsertType.LIST,
            collection=True,
            doc_id=KeyStyle.TEXT_FIELD,
            id_key="event_id")

    dbm.cb_map_upsert("testdata", cfg, xml_data=xml_data)
    print("=> Cleanup")
    dbm.drop_bucket(bucket)


def manual_4(hostname, bucket, tls, scope, collection, file):
    dbm = CBManager(hostname, "Administrator", "password", ssl=False).connect()
    dbm.create_bucket(bucket)
    dbm.create_scope(scope)
    dbm.create_collection(collection)

    print(f"=> Map Test File {file}")
    cfg = UpsertMapConfig().new()
    cfg.add('root.addresses.billing', collection=True)
    cfg.add('root.addresses.delivery', collection=True)
    cfg.add('root.history.events',
            p_type=MapUpsertType.LIST,
            collection=True,
            doc_id=KeyStyle.TEXT_FIELD,
            id_key="event_id")

    base = os.path.basename(file)
    prefix = os.path.splitext(base)[0]
    print(f"=> Doc ID Prefix {prefix}")

    dbm.cb_map_upsert(prefix, cfg, xml_file=file)


def manual_5(hostname, bucket, tls, scope, collection):
    try:
        op = CBOperation(hostname, "Administrator", "password", ssl=False).connect().bucket(bucket).scope(scope).collection(collection)
    except (BucketNotFoundException, ScopeNotFoundException, CollectionNotFoundException):
        pass

    op = CBOperation(hostname, "Administrator", "password", ssl=False, quota=128, create=True).connect().bucket(bucket).scope(scope).collection(collection)

    op.put_doc("test::1", document)
    d = op.get_doc("test::1")
    assert d == document


p = Params()
options = p.parameters

try:
    debug_level = int(os.environ['DEBUG_LEVEL'])
except (ValueError, KeyError):
    debug_level = 3

if debug_level == 0 or options.verbose:
    logger.setLevel(logging.DEBUG)
elif debug_level == 1:
    logger.setLevel(logging.ERROR)
elif debug_level == 2:
    logger.setLevel(logging.INFO)
else:
    logger.setLevel(logging.CRITICAL)

logging.basicConfig()

if options.start:
    container_start()
    sys.exit(0)

if options.stop:
    container_stop()
    sys.exit(0)

if options.file:
    manual_4(options.host, "import", options.ssl, "_default", "_default", options.file)
    sys.exit(0)

if options.pool:
    manual_5(options.host, "test", options.ssl, "test", "test")
    sys.exit(0)

manual_1(options.host, "test", options.ssl, "test", "test")
manual_2(options.host, "testa", options.ssl, "test", "test")
manual_3(options.host, "testb", options.ssl, "test", "test")
