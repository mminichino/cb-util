#!/usr/bin/env python3

import warnings
import sys
import argparse
import os
import logging
import json
import string
import time
from couchbase.exceptions import (BucketNotFoundException, ScopeNotFoundException, CollectionNotFoundException)

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
sys.path.append(current)

from cbcmgr.cb_connect import CBConnect
from cbcmgr.cb_management import CBManager
from cbcmgr.cb_bucket import Bucket
from cbcmgr.cb_operation_s import CBOperation, Operation
from cbcmgr.mt_pool import CBPool
from cbcmgr.config import UpsertMapConfig, MapUpsertType, KeyStyle
from cbcmgr.cb_pathmap import CBPathMap
from tests.common import start_container, stop_container, run_in_container, get_container_id, document, new_document, query_result, json_data, xml_data, image_name

logger = logging.getLogger()

warnings.filterwarnings("ignore")


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
        parser.add_argument("--basic", action="store_true")
        parser.add_argument("--map", action="store_true")
        parser.add_argument("--verbose", action="store_true")
        parser.add_argument("--file", action="store", help="Input File")
        self.args = parser.parse_args()

    @property
    def parameters(self):
        return self.args


def container_start():
    print("Starting test container")
    platform = f"linux/{os.uname().machine}"
    container_id = start_container(image_name, platform)
    command = ['/bin/bash', '-c', 'test -f /demo/couchbase/.ready']
    while not run_in_container(container_id, command):
        time.sleep(1)
    command = ['cbcutil', 'list', '--host', '127.0.0.1', '--wait']
    run_in_container(container_id, command)
    time.sleep(1)


def container_stop():
    print("Stopping test container")
    container_id = get_container_id()
    stop_container(container_id)
    time.sleep(1)


def manual_1(hostname, bucket_name, tls, scope, collection):
    replica_count = 0
    bucket = Bucket(**dict(
        name=bucket_name,
        num_replicas=0
    ))

    print("=> Connect")
    dbm = CBManager(hostname, "Administrator", "password", ssl=tls).connect()
    dbm.create_bucket(bucket)
    dbm.create_scope(scope)
    dbm.create_collection(collection)
    dbc = CBConnect(hostname, "Administrator", "password", ssl=tls).connect(bucket_name, scope, collection)
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
    dbc.bucket_wait(bucket_name, count=1)
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
    dbm.drop_bucket(bucket_name)


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
    dbm = CBManager(hostname, "Administrator", "password", ssl=tls).connect()
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
    keyspace = f"{bucket}.{scope}.{collection}"
    try:
        print(f"=> Test Exception")
        opm = CBOperation(hostname, "Administrator", "password", ssl=False)
        col_a = opm.connect(keyspace)
    except (BucketNotFoundException, ScopeNotFoundException, CollectionNotFoundException):
        pass

    print(f"=> Test Operator Class")
    col_a = CBOperation(hostname, "Administrator", "password", ssl=False, quota=128, create=True).connect(keyspace)

    col_a.put_doc(col_a.collection, "test::1", document)
    d = col_a.get_doc(col_a.collection, "test::1")
    assert d == document

    col_a.index_by_query("select data from test.test.test")

    r = col_a.run_query(col_a.cluster, "select data from test.test.test")
    assert r[0]['data'] == 'data'

    print(f"=> Test Cleanup")
    col_a.cleanup()

    print(f"=> Test Operators")
    col_t = CBOperation(hostname, "Administrator", "password", ssl=False, quota=128, create=True).connect(keyspace)
    a_read = col_t.get_operator(Operation.READ)
    a_write = col_t.get_operator(Operation.WRITE)
    a_query = col_t.get_operator(Operation.QUERY)

    a_write.prep("test::1", document)
    a_write.execute()
    a_read.prep("test::1")
    a_read.execute()
    assert document == a_read.result["test::1"]

    col_t.index_by_query("select data from test.test.test")
    a_query.prep("select data from test.test.test")
    a_query.execute()
    assert a_query.result[0]['data'] == 'data'

    print(f"=> Test Cleanup")
    col_a.cleanup()


def manual_6(hostname, bucket, tls, scope, collection):
    print(f"=> Pool Test 1")
    pool = CBPool(hostname, "Administrator", "password", ssl=False, quota=128, create=True)

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
    manual_6(options.host, "test", options.ssl, "_default", "test")
    sys.exit(0)

if options.map:
    manual_2(options.host, "test", options.ssl, "test", "test")
    manual_3(options.host, "test", options.ssl, "test", "test")
    sys.exit(0)

if options.basic:
    manual_1(options.host, "test", options.ssl, "test", "test")
