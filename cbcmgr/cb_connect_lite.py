##
##

from .exceptions import (IndexInternalError, CollectionGetError, CollectionCountError)
from .retry import retry
from .cb_session import CBSession, BucketMode
import logging
import hashlib
from datetime import timedelta
from typing import Union, Dict, Any, List
from couchbase.cluster import Cluster
from couchbase.bucket import Bucket
from couchbase.scope import Scope
from couchbase.collection import Collection
from couchbase.management.buckets import CreateBucketSettings, BucketType, StorageBackend
from couchbase.management.collections import CollectionSpec
from couchbase.management.options import CreateQueryIndexOptions, CreatePrimaryQueryIndexOptions, WatchQueryIndexOptions
from couchbase.exceptions import (BucketNotFoundException, ScopeNotFoundException, CollectionNotFoundException, BucketAlreadyExistsException, ScopeAlreadyExistsException,
                                  CollectionAlreadyExistsException, QueryIndexAlreadyExistsException, DocumentNotFoundException, WatchQueryIndexTimeoutException)

logger = logging.getLogger('cbutil.connect.lite')
logger.addHandler(logging.NullHandler())
JSONType = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]


class CBConnectLite(CBSession):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @retry(always_raise_list=(BucketNotFoundException,))
    def get_bucket(self, cluster: Cluster, name: str) -> Bucket:
        if name is None:
            raise TypeError("name can not be None")
        logger.debug(f"bucket: connect {name}")
        return cluster.bucket(name)

    @retry()
    def create_bucket(self, cluster: Cluster, name: str, quota: int = 256, replicas: int = 0, mode: BucketMode = BucketMode.DEFAULT):
        if name is None:
            raise TypeError("name can not be None")

        if mode == BucketMode.DEFAULT:
            b_type = BucketType.COUCHBASE
            b_stor = StorageBackend.COUCHSTORE
        elif mode == BucketMode.CACHE:
            b_type = BucketType.EPHEMERAL
            b_stor = StorageBackend.COUCHSTORE
        else:
            b_type = BucketType.COUCHBASE
            b_stor = StorageBackend.MAGMA

        logger.debug(f"creating bucket {name} type {b_type.name} storage {b_stor.name} replicas {replicas} quota {quota}")

        try:
            bm = cluster.buckets()
            bm.create_bucket(CreateBucketSettings(name=name,
                                                  bucket_type=b_type,
                                                  storage_backend=b_stor,
                                                  num_replicas=replicas,
                                                  ram_quota_mb=quota))
        except BucketAlreadyExistsException:
            pass

    @retry(always_raise_list=(ScopeNotFoundException,))
    def get_scope(self, bucket: Bucket, name: str = "_default") -> Scope:
        if name is None:
            raise TypeError("name can not be None")
        logger.debug(f"scope: connect {name}")
        if not self.is_scope(bucket, name):
            raise ScopeNotFoundException(f"scope {name} does not exist")
        return bucket.scope(name)

    @retry()
    def create_scope(self, bucket: Bucket, name: str):
        if name is None:
            raise TypeError("name can not be None")

        try:
            if name != "_default":
                cm = bucket.collections()
                cm.create_scope(name)
        except ScopeAlreadyExistsException:
            pass

    @retry(always_raise_list=(CollectionNotFoundException,))
    def get_collection(self, bucket: Bucket, scope: Scope, name: str = "_default") -> Collection:
        if name is None:
            raise TypeError("name can not be None")
        logger.debug(f"collection: connect {name}")
        if not self.is_collection(bucket, scope.name, name):
            raise CollectionNotFoundException(f"collection {name} does not exist")
        return scope.collection(name)

    @retry()
    def create_collection(self, bucket: Bucket, scope: Scope, name: str):
        if name is None:
            raise TypeError("name can not be None")

        try:
            if name != "_default":
                collection_spec = CollectionSpec(name, scope_name=scope.name)
                cm = bucket.collections()
                cm.create_collection(collection_spec)
        except CollectionAlreadyExistsException:
            pass

    @staticmethod
    def try_collection(bucket: Bucket, name: str):
        try:
            collection = bucket.collection(name)
            collection.exists("null")
        except Exception as err:
            raise CollectionGetError(f"collection {name}: key exists error: {err}")

    @retry()
    def collection_count(self, cluster: Cluster, keyspace: str) -> int:
        try:
            sql = 'select count(*) as count from ' + keyspace + ';'
            result = self.run_query(cluster, sql)
            count: int = int(result[0]['count'])
            return count
        except Exception as err:
            raise CollectionCountError(f"failed to get count for {keyspace}: {err}")

    @retry()
    def run_query(self, cluster: Cluster, sql: str):
        contents = []
        result = cluster.query(sql)
        for item in result:
            contents.append(item)
        return contents

    @retry(always_raise_list=(DocumentNotFoundException, ScopeNotFoundException, CollectionNotFoundException))
    def get_doc(self, collection: Collection, doc_id: str):
        result = collection.get(doc_id)
        return result.content_as[dict]

    @retry(always_raise_list=(ScopeNotFoundException, CollectionNotFoundException))
    def put_doc(self, collection: Collection, doc_id: str, document: JSONType):
        result = collection.upsert(doc_id, document)
        return result.cas

    def index_by_query(self, sql: str):
        advisor = f"select advisor([\"{sql}\"])"
        cluster: Cluster = self.session()

        results = self.run_query(cluster, advisor)

        current = results[0].get('$1', {}).get('current_used_indexes')
        if current:
            logger.debug("index already exists")
            return

        try:
            index_list = results[0]['$1']['recommended_indexes']
            for item in index_list:
                index_query = item['index']
                logger.debug(f"creating index: {index_query}")
                self.run_query(cluster, index_query)
        except (KeyError, ValueError):
            logger.debug(f"can not get recommended index from query {advisor}")
            raise IndexInternalError(f"can not determine index for query")

    @retry()
    def create_indexes(self, cluster: Cluster, bucket: Bucket, scope: Scope, collection: Collection, fields: List[str], replica: int = 0):
        if collection.name != '_default':
            index_options = CreateQueryIndexOptions(deferred=False,
                                                    num_replicas=replica,
                                                    collection_name=collection.name,
                                                    scope_name=scope.name)
        else:
            index_options = CreateQueryIndexOptions(deferred=False,
                                                    num_replicas=replica)
        try:
            qim = cluster.query_indexes()
            for field in fields:
                hash_string = f"{bucket.name}_{scope.name}_{collection.name}_{field}"
                name_part = hashlib.shake_256(hash_string.encode()).hexdigest(3)
                index_name = f"{field}_{name_part}_ix"
                logger.debug(f"creating index {index_name} on {field} for {collection.name}")
                qim.create_index(bucket.name, index_name, [field], index_options)
                self.index_wait(cluster, bucket, scope, collection, index_name)
        except QueryIndexAlreadyExistsException:
            logger.debug(f"index already exists")
            pass

    @retry()
    def create_primary_index(self, cluster: Cluster, bucket: Bucket, scope: Scope, collection: Collection, replica: int = 0):
        if collection.name != '_default':
            index_options = CreatePrimaryQueryIndexOptions(deferred=False,
                                                           num_replicas=replica,
                                                           collection_name=collection.name,
                                                           scope_name=scope.name)
        else:
            index_options = CreatePrimaryQueryIndexOptions(deferred=False,
                                                           num_replicas=replica)
        logger.debug(f"creating primary index on {collection.name}")
        try:
            qim = cluster.query_indexes()
            qim.create_primary_index(bucket.name, index_options)
            self.index_wait_primary(cluster, bucket)
        except QueryIndexAlreadyExistsException:
            pass

    @retry(always_raise_list=(WatchQueryIndexTimeoutException,))
    def index_wait(self, cluster: Cluster, bucket: Bucket, scope: Scope, collection: Collection, index: str):
        watch_options = WatchQueryIndexOptions(
            collection_name=collection.name,
            scope_name=scope.name,
            timeout=timedelta(seconds=10)
        )
        qim = cluster.query_indexes()
        qim.watch_indexes(bucket.name, [index], watch_options)

    @retry(always_raise_list=(WatchQueryIndexTimeoutException,))
    def index_wait_primary(self, cluster: Cluster, bucket: Bucket):
        watch_options = WatchQueryIndexOptions(
            watch_primary=True,
            timeout=timedelta(seconds=10)
        )
        qim = cluster.query_indexes()
        qim.watch_indexes(bucket.name, [], watch_options)

    @staticmethod
    def is_scope(bucket: Bucket, name: str):
        if name is None:
            raise TypeError("name can not be None")
        cm = bucket.collections()
        return next((s for s in cm.get_all_scopes() if s.name == name), None)

    @staticmethod
    def is_collection(bucket: Bucket, scope: str, name: str):
        if name is None or scope is None:
            raise TypeError("name and scope can not be None")
        cm = bucket.collections()
        sm = next((s for s in cm.get_all_scopes() if s.name == scope), None)
        return next((i for i in sm.collections if i.name == name), None)
