#!/usr/bin/env python3

import warnings
import sys
import argparse
import os

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
sys.path.append(current)

from cbcmgr.cb_connect import CBConnect
from conftest import pytest_sessionstart, pytest_sessionfinish


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
    db = CBConnect(hostname, "Administrator", "password", ssl=tls)\
        .connect()\
        .create_bucket(bucket)\
        .create_scope(scope)\
        .create_collection(collection)
    print("=> Create indexes")
    db.cb_create_primary_index(replica=replica_count)
    index_name = db.cb_create_index(fields=["data"], replica=replica_count)
    db.index_wait()
    db.index_wait(index_name)
    result = db.is_index()
    assert result is True
    result = db.is_index(index_name)
    assert result is True
    db.cb_upsert("test::1", document)
    db.bucket_wait(bucket, count=1)
    print("=> Data tests")
    result = db.cb_get("test::1")
    assert result == document
    result = db.collection_count(expect_count=1)
    assert result == 1
    result = db.cb_query(field="data", empty_retry=True)
    assert result == query_result
    db.cb_upsert("test::2", document)
    db.cb_subdoc_multi_upsert(["test::1", "test::2"], "data", ["new", "new"])
    result = db.cb_get("test::1")
    assert result == new_document
    result = db.collection_count(expect_count=2)
    assert result == 2
    db.cb_upsert("test::3", document)
    db.cb_subdoc_upsert("test::3", "data", "new")
    result = db.cb_get("test::3")
    assert result == new_document
    print("=> Cleanup")
    db.cb_drop_primary_index()
    db.cb_drop_index(index_name)
    db.delete_wait()
    db.delete_wait(index_name)
    db.drop_bucket(bucket)


p = Params()
options = p.parameters

if options.start:
    container_start()
    sys.exit(0)

if options.stop:
    container_stop()
    sys.exit(0)

manual_1(options.host, "test", options.ssl, "test", "test")
