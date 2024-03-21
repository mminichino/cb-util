#!/usr/bin/env python3

import os
import warnings
import pytest
import asyncio
import string
import time
from couchbase.exceptions import (BucketNotFoundException, ScopeNotFoundException, CollectionNotFoundException)
from cbcmgr.cb_connect_lite_a import CBConnectLiteAsync
from cbcmgr.cb_operation_a import CBOperationAsync, Operation
from cbcmgr.async_pool import CBPoolAsync
from cbcmgr.cli.system import SysInfo
from tests.common import start_container, stop_container, run_in_container, document, image_name


warnings.filterwarnings("ignore")


@pytest.mark.serial
class TestAsyncDrv1(object):
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
    @pytest.mark.parametrize("bucket_name", ["test"])
    @pytest.mark.parametrize("scope_name, collection_name", [("_default", "_default"), ("test", "test")])
    @pytest.mark.parametrize("tls", [False, True])
    @pytest.mark.asyncio
    async def test_1(self, hostname, bucket_name, tls, scope_name, collection_name):
        replica_count = 0
        keyspace = f"{bucket_name}.{scope_name}.{collection_name}"

        ca = CBConnectLiteAsync(hostname, "Administrator", "password", ssl=tls)
        cluster = await ca.session_a()

        await ca.create_bucket(cluster, bucket_name, quota=128, replicas=0)
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

        time.sleep(1)
        result = await ca.collection_count(cluster, keyspace)
        assert result == 1

        result = await ca.run_query(cluster, f"select data from {keyspace}")
        assert result[0]['data'] == 'data'

        bm = cluster.buckets()
        await bm.drop_bucket(bucket_name)
        await ca.end_session_a(cluster)

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    @pytest.mark.parametrize("bucket_name", ["test"])
    @pytest.mark.parametrize("scope_name, collection_name", [("_default", "_default"), ("test", "test")])
    @pytest.mark.parametrize("tls", [False, True])
    @pytest.mark.asyncio
    async def test_2(self, hostname, bucket_name, tls, scope_name, collection_name):
        keyspace = f"{bucket_name}.{scope_name}.{collection_name}"
        try:
            opc = CBOperationAsync(hostname, "Administrator", "password", ssl=tls, replicas=0)
            opm = await opc.init()
            col_a = await opm.connect(keyspace)
            col_a.cleanup()
        except (BucketNotFoundException, ScopeNotFoundException, CollectionNotFoundException):
            pass

        opc = CBOperationAsync(hostname, "Administrator", "password", ssl=tls, quota=128, create=True, replicas=0)
        opm = await opc.init()
        col_a = await opm.connect(keyspace)

        await col_a.put_doc(col_a.collection, "test::1", document)
        d = await col_a.get_doc(col_a.collection, "test::1")
        assert d == document

        await col_a.index_by_query(f"select data from {keyspace}")

        r = await col_a.run_query(col_a.cluster, f"select data from {keyspace}")
        assert r[0]['data'] == 'data'

        await col_a.cleanup()

        opc = CBOperationAsync(hostname, "Administrator", "password", ssl=tls, quota=128, create=True, replicas=0)
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
        await col_a.close()


@pytest.mark.serial
class TestAsyncDrv2(object):
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
    @pytest.mark.asyncio
    async def test_1(self, hostname, bucket, tls, scope, collection):
        pool = CBPoolAsync(hostname, "Administrator", "password", ssl=tls, quota=128, create=True, replicas=0)

        for n in range(10):
            c = string.ascii_lowercase[n:n + 1]
            keyspace = f"{bucket}.{scope}.{collection}{c}"
            await pool.connect(keyspace)
            for i in range(1000):
                await pool.dispatch(keyspace, Operation.WRITE, f"test::{i + 1}", document)

        await pool.join()
        await pool.shutdown()
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

        opc = CBOperationAsync(hostname, "Administrator", "password", ssl=tls, quota=128, create=True, replicas=0)
        opm = await opc.init()
        keyspace = f"{bucket}.{scope}.{collection}"
        col_a = await opm.connect(keyspace)
        await col_a.cleanup()
