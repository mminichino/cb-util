#!/usr/bin/env python3

import warnings
import sys
import argparse
import os
import logging
import json
import string
import asyncio
from couchbase.exceptions import (BucketNotFoundException, ScopeNotFoundException, CollectionNotFoundException)

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
sys.path.append(current)

from cbcmgr.cb_management import CBManager
from cbcmgr.cb_bucket import Bucket
from cbcmgr.cb_operation_a import CBOperationAsync, Operation
from cbcmgr.cb_operation_s import CBOperation
from cbcmgr.cb_connect_lite_a import CBConnectLiteAsync
from cbcmgr.async_pool import CBPoolAsync
from cbcmgr.config import UpsertMapConfig, MapUpsertType, KeyStyle
from cbcmgr.cb_pathmap import CBPathMap
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
        parser.add_argument("--map", action="store_true")
        parser.add_argument("--base", action="store_true")
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


def unhandled_exception(_, context):
    err = context.get("exception", context['message'])
    if isinstance(err, Exception):
        print(f"unhandled exception: type: {err.__class__.__name__} msg: {err} cause: {err.__cause__}")
    else:
        print(f"unhandled error: {err}")


async def manual_1(hostname, bucket_name, tls, scope_name, collection_name):
    replica_count = 0
    keyspace = f"{bucket_name}.{scope_name}.{collection_name}"

    print("=> Connect")
    ca = CBConnectLiteAsync(hostname, "Administrator", "password", ssl=tls)
    cluster = await ca.session_a()

    print("=> Create Bucket")
    await ca.create_bucket(cluster, bucket_name, quota=128)
    bucket = await ca.get_bucket(cluster, bucket_name)
    await ca.create_scope(bucket, scope_name)
    scope = await ca.get_scope(bucket, scope_name)
    await ca.create_collection(bucket, scope, collection_name)
    collection = await ca.get_collection(bucket, scope, collection_name)

    print("=> Create indexes")
    await ca.create_primary_index(cluster, bucket, scope, collection, replica=replica_count)
    await ca.create_indexes(cluster, bucket, scope, collection, fields=["data"], replica=replica_count)

    print("=> Data Operations")
    await ca.put_doc(collection, "test::1", document)
    result = await ca.get_doc(collection, "test::1")
    assert result == document

    result = await ca.collection_count(cluster, keyspace)
    assert result == 1

    result = await ca.run_query(cluster, f"select data from {keyspace}")
    assert result[0]['data'] == 'data'

    print("=> Cleanup")
    bm = cluster.buckets()
    await bm.drop_bucket(bucket_name)


def manual_2(hostname, bucket, tls, scope, collection):
    print("=> Map Test JSON")
    cfg = UpsertMapConfig().new()
    cfg.add('addresses.billing')
    cfg.add('addresses.delivery')
    cfg.add('history.events',
            p_type=MapUpsertType.LIST,
            id_key="event_id")

    p_map = CBPathMap(cfg, hostname, "Administrator", "password", bucket, scope, ssl=False, quota=128)
    p_map.load_data("testdata", json_data=json.dumps(json_data, indent=2))
    print("=> Cleanup")
    CBOperation(hostname, "Administrator", "password", ssl=tls).connect(bucket).cleanup()


def manual_3(hostname, bucket, tls, scope, collection):
    print("=> Map Test XML")
    cfg = UpsertMapConfig().new()
    cfg.add('root.addresses.billing')
    cfg.add('root.addresses.delivery')
    cfg.add('root.history.events',
            p_type=MapUpsertType.LIST,
            id_key="event_id")

    p_map = CBPathMap(cfg, hostname, "Administrator", "password", bucket, scope, ssl=False, quota=128)
    p_map.load_data("testdata", xml_data=xml_data)
    print("=> Cleanup")
    CBOperation(hostname, "Administrator", "password", ssl=tls).connect(bucket).cleanup()


def manual_4(hostname, bucket_name, tls, scope, collection, file):
    bucket = Bucket(**dict(
        name=bucket_name,
        num_replicas=0
    ))
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


async def manual_5(hostname, bucket, tls, scope, collection):
    keyspace = f"{bucket}.{scope}.{collection}"
    try:
        print(f"=> Test Exception")
        opc = CBOperationAsync(hostname, "Administrator", "password", ssl=tls)
        opm = await opc.init()
        col_a = await opm.connect(keyspace)
        col_a.cleanup()
    except (BucketNotFoundException, ScopeNotFoundException, CollectionNotFoundException):
        pass

    print(f"=> Test Operator Class")
    opc = CBOperationAsync(hostname, "Administrator", "password", ssl=tls, quota=128, create=True)
    opm = await opc.init()
    col_a = await opm.connect(keyspace)

    await col_a.put_doc(col_a.collection, "test::1", document)
    d = await col_a.get_doc(col_a.collection, "test::1")
    assert d == document

    await col_a.index_by_query("select data from test.test.test")

    r = await col_a.run_query(col_a.cluster, "select data from test.test.test")
    assert r[0]['data'] == 'data'

    print(f"=> Test Cleanup")
    await col_a.cleanup()

    print(f"=> Test Operators")
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

    await col_t.index_by_query("select data from test.test.test")
    a_query.prep("select data from test.test.test")
    await a_query.execute()
    assert a_query.result[0]['data'] == 'data'

    print(f"=> Test Cleanup")
    await col_a.cleanup()


async def manual_6(hostname, bucket, tls, scope, collection):
    print(f"=> Pool Test 1")
    pool = CBPoolAsync(hostname, "Administrator", "password", ssl=False, quota=128, create=True)

    for n in range(10):
        c = string.ascii_lowercase[n:n + 1]
        keyspace = f"{bucket}.{scope}.{collection}{c}"
        await pool.connect(keyspace)
        for i in range(1000):
            await pool.dispatch(keyspace, Operation.WRITE, f"test::{i+1}", document)

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
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(unhandled_exception)
    loop.run_until_complete(manual_5(options.host, "test", options.ssl, "test", "test"))
    loop.run_until_complete(manual_6(options.host, "test", options.ssl, "test", "test"))
    sys.exit(0)

if options.map:
    manual_2(options.host, "test", options.ssl, "test", "test")
    manual_3(options.host, "test", options.ssl, "test", "test")
    sys.exit(0)

if options.base:
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(unhandled_exception)
    loop.run_until_complete(manual_1(options.host, "test", options.ssl, "_default", "_default"))
