#!/usr/bin/env python3

import warnings
import pytest
from cbutil.cb_connect import CBConnect


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


@pytest.mark.parametrize("scope, collection", [("_default", "_default"), ("test", "test")])
@pytest.mark.parametrize("tls", [False, True])
def test_cb_driver_1(hostname, bucket, tls, scope, collection):
    replica_count = 0

    db = CBConnect(hostname, "Administrator", "password", ssl=False) \
        .connect() \
        .create_bucket(bucket) \
        .create_scope(scope) \
        .create_collection(collection)

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

    db.cb_drop_primary_index()
    db.cb_drop_index(index_name)
    db.delete_wait()
    db.delete_wait(index_name)
    db.drop_bucket(bucket)
