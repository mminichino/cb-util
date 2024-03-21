#!/usr/bin/env python3

import os
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
from cbcmgr.cli.system import SysInfo
from tests.common import start_container, stop_container, run_in_container, document, new_document, query_result, json_data, xml_data, image_name


warnings.filterwarnings("ignore")


@pytest.mark.serial
class TestSyncDrv1(object):
    container_id = None

    @classmethod
    def setup_class(cls):
        SysInfo().raise_nofile()
        print("Starting test container")
        platform = f"linux/{os.uname().machine}"
        cls.container_id = start_container(image_name, platform)
        command = ['/bin/bash', '-c', 'test -f /demo/couchbase/.ready']
        while not run_in_container(cls.container_id, command):
            time.sleep(1)
        command = ['cbcutil', 'list', '--host', '127.0.0.1', '--wait']
        run_in_container(cls.container_id, command)
        time.sleep(1)

    @classmethod
    def teardown_class(cls):
        print("Stopping test container")
        stop_container(cls.container_id)
        time.sleep(1)

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    @pytest.mark.parametrize("bucket", ["test"])
    @pytest.mark.parametrize("scope, collection", [("_default", "_default"), ("test", "test")])
    @pytest.mark.parametrize("tls", [False, True])
    def test_1(self, hostname, bucket, tls, scope, collection):
        replica_count = 0
        bucket_opts = Bucket(**dict(
            name=bucket,
            num_replicas=0
        ))

        dbm = CBManager(hostname, "Administrator", "password", ssl=tls).connect()
        dbm.create_bucket(bucket_opts)
        dbm.create_scope(scope)
        dbm.create_collection(collection)
        result = dbm.get_bucket(bucket)
        assert result is not None

        dbc = CBConnect(hostname, "Administrator", "password", ssl=tls).connect(bucket, scope, collection)

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
        dbm.close()

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    @pytest.mark.parametrize("bucket", ["test"])
    @pytest.mark.parametrize("scope, collection", [("_default", "_default"), ("test", "test")])
    @pytest.mark.parametrize("tls", [False, True])
    def test_2(self, hostname, bucket, tls, scope, collection):
        keyspace = f"{bucket}.{scope}.{collection}"
        try:
            opm = CBOperation(hostname, "Administrator", "password", ssl=tls, replicas=0)
            col_a = opm.connect(keyspace)
            col_a.cleanup()
        except (BucketNotFoundException, ScopeNotFoundException, CollectionNotFoundException):
            pass

        col_a = CBOperation(hostname, "Administrator", "password", ssl=tls, quota=128, create=True, replicas=0).connect(keyspace)

        col_a.put_doc(col_a.collection, "test::1", document)
        d = col_a.get_doc(col_a.collection, "test::1")
        assert d == document

        col_a.index_by_query(f"select data from {keyspace}")

        r = col_a.run_query(col_a.cluster, f"select data from {keyspace}")
        assert r[0]['data'] == 'data'

        col_a.cleanup()

        col_t = CBOperation(hostname, "Administrator", "password", ssl=tls, quota=128, create=True, replicas=0).connect(keyspace)
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
        col_a.close()


@pytest.mark.serial
class TestSyncDrv2(object):
    container_id = None

    @classmethod
    def setup_class(cls):
        SysInfo().raise_nofile()
        print("Starting test container")
        platform = f"linux/{os.uname().machine}"
        cls.container_id = start_container(image_name, platform)
        command = ['/bin/bash', '-c', 'test -f /demo/couchbase/.ready']
        while not run_in_container(cls.container_id, command):
            time.sleep(1)
        command = ['cbcutil', 'list', '--host', '127.0.0.1', '--wait']
        run_in_container(cls.container_id, command)
        time.sleep(1)

    @classmethod
    def teardown_class(cls):
        print("Stopping test container")
        stop_container(cls.container_id)
        time.sleep(1)

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    @pytest.mark.parametrize("bucket", ["test"])
    @pytest.mark.parametrize("scope", ["_default", "test"])
    @pytest.mark.parametrize("tls", [False, True])
    def test_1(self, hostname, bucket, tls, scope):
        cfg = UpsertMapConfig().new()
        cfg.add('addresses.billing')
        cfg.add('addresses.delivery')
        cfg.add('history.events',
                p_type=MapUpsertType.LIST,
                id_key="event_id")

        p_map = CBPathMap(cfg, hostname, "Administrator", "password", bucket, scope, ssl=tls, quota=128, replicas=0)
        p_map.connect()
        p_map.load_data("testdata", json_data=json.dumps(json_data, indent=2))
        CBOperation(hostname, "Administrator", "password", ssl=tls).connect(bucket).cleanup()

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    @pytest.mark.parametrize("bucket", ["test"])
    @pytest.mark.parametrize("scope", ["_default", "test"])
    @pytest.mark.parametrize("tls", [False, True])
    def test_2(self, hostname, bucket, tls, scope):
        cfg = UpsertMapConfig().new()
        cfg.add('root.addresses.billing')
        cfg.add('root.addresses.delivery')
        cfg.add('root.history.events',
                p_type=MapUpsertType.LIST,
                id_key="event_id")

        p_map = CBPathMap(cfg, hostname, "Administrator", "password", bucket, scope, ssl=False, quota=128, replicas=0)
        p_map.connect()
        p_map.load_data("testdata", xml_data=xml_data)
        CBOperation(hostname, "Administrator", "password", ssl=tls).connect(bucket).cleanup()


@pytest.mark.serial
class TestSyncDrv3(object):
    container_id = None

    @classmethod
    def setup_class(cls):
        SysInfo().raise_nofile()
        print("Starting test container")
        platform = f"linux/{os.uname().machine}"
        cls.container_id = start_container(image_name, platform)
        command = ['/bin/bash', '-c', 'test -f /demo/couchbase/.ready']
        while not run_in_container(cls.container_id, command):
            time.sleep(1)
        command = ['cbcutil', 'list', '--host', '127.0.0.1', '--wait']
        run_in_container(cls.container_id, command)
        time.sleep(1)

    @classmethod
    def teardown_class(cls):
        print("Stopping test container")
        stop_container(cls.container_id)
        time.sleep(1)

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    @pytest.mark.parametrize("bucket", ["test"])
    @pytest.mark.parametrize("scope", ["_default", "test"])
    @pytest.mark.parametrize("collection", ["test"])
    @pytest.mark.parametrize("tls", [False, True])
    def test_1(self, hostname, bucket, tls, scope, collection):
        pool = CBPool(hostname, "Administrator", "password", ssl=tls, quota=128, create=True, replicas=0)

        for n in range(10):
            c = string.ascii_lowercase[n:n + 1]
            keyspace = f"{bucket}.{scope}.{collection}{c}"
            pool.connect(keyspace)
            for i in range(1000):
                pool.dispatch(keyspace, Operation.WRITE, f"test::{i+1}", document)

        pool.join()
        pool.shutdown()
        time.sleep(1)
        count = 0
        for n in range(10):
            c = string.ascii_lowercase[n:n + 1]
            keyspace = f"{bucket}.{scope}.{collection}{c}"
            count += CBOperation(hostname, "Administrator", "password", ssl=tls).connect(keyspace).get_count()
        assert count == 10000

        CBOperation(hostname, "Administrator", "password", ssl=tls).connect(bucket).cleanup()
        time.sleep(1)
