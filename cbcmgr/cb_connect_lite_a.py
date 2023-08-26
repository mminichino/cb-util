##
##
import asyncio

from .exceptions import (IndexInternalError, CollectionCountError, BucketStatsError)
from .retry import retry
from .cb_session import CBSession, BucketMode
from .httpsessionmgr import APISession
import logging
import hashlib
from datetime import timedelta
from typing import Union, Dict, Any, List
from acouchbase.cluster import AsyncCluster
from acouchbase.bucket import AsyncBucket
from acouchbase.scope import AsyncScope
from acouchbase.collection import AsyncCollection
from couchbase.management.buckets import CreateBucketSettings, BucketType, StorageBackend
from couchbase.management.collections import CollectionSpec
from couchbase.management.options import CreateQueryIndexOptions, CreatePrimaryQueryIndexOptions, WatchQueryIndexOptions
from couchbase.exceptions import (BucketNotFoundException, ScopeNotFoundException, CollectionNotFoundException, BucketAlreadyExistsException, ScopeAlreadyExistsException,
                                  CollectionAlreadyExistsException, QueryIndexAlreadyExistsException, DocumentNotFoundException, WatchQueryIndexTimeoutException)

logger = logging.getLogger('cbutil.connect.lite')
logger.addHandler(logging.NullHandler())
JSONType = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]


class CBConnectLiteAsync(CBSession):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @retry(always_raise_list=(BucketNotFoundException,))
    async def get_bucket(self, cluster: AsyncCluster, name: str) -> AsyncBucket:
        if name is None:
            raise TypeError("name can not be None")
        logger.debug(f"bucket: connect {name}")
        bucket = cluster.bucket(name)
        await bucket.on_connect()
        return bucket

    @retry()
    async def create_bucket(self, cluster: AsyncCluster, name: str, quota: int = 256, replicas: int = 0, mode: BucketMode = BucketMode.DEFAULT):
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
            await bm.create_bucket(CreateBucketSettings(name=name,
                                                        bucket_type=b_type,
                                                        storage_backend=b_stor,
                                                        num_replicas=replicas,
                                                        ram_quota_mb=quota))
        except BucketAlreadyExistsException:
            pass

    @retry(always_raise_list=(ScopeNotFoundException,))
    async def get_scope(self, bucket: AsyncBucket, name: str = "_default") -> AsyncScope:
        if name is None:
            raise TypeError("name can not be None")
        logger.debug(f"scope: connect {name}")
        if not await self.is_scope(bucket, name):
            raise ScopeNotFoundException(f"scope {name} does not exist")
        scope = bucket.scope(name)
        return scope

    @retry()
    async def create_scope(self, bucket: AsyncBucket, name: str):
        if name is None:
            raise TypeError("name can not be None")

        logger.debug(f"scope: create {name}")
        try:
            if name != "_default":
                cm = bucket.collections()
                await cm.create_scope(name)
        except ScopeAlreadyExistsException:
            pass

    @retry(always_raise_list=(CollectionNotFoundException,))
    async def get_collection(self, bucket: AsyncBucket, scope: AsyncScope, name: str = "_default") -> AsyncCollection:
        if name is None:
            raise TypeError("name can not be None")
        logger.debug(f"collection: connect {name}")
        if not await self.is_collection(bucket, scope.name, name):
            raise CollectionNotFoundException(f"collection {name} does not exist")
        collection = scope.collection(name)
        return collection

    @retry()
    async def create_collection(self, bucket: AsyncBucket, scope: AsyncScope, name: str):
        if name is None:
            raise TypeError("name can not be None")

        logger.debug(f"collection: create {name}")
        try:
            if name != "_default":
                collection_spec = CollectionSpec(name, scope_name=scope.name)
                cm = bucket.collections()
                await cm.create_collection(collection_spec)
        except CollectionAlreadyExistsException:
            pass

    @retry()
    async def collection_count(self, cluster: AsyncCluster, keyspace: str) -> int:
        try:
            sql = 'select count(*) as count from ' + keyspace + ';'
            result = await self.run_query(cluster, sql)
            count: int = int(result[0]['count'])
            return count
        except Exception as err:
            raise CollectionCountError(f"failed to get count for {keyspace}: {err}")

    @retry()
    def bucket_stats(self, name):
        try:
            s = APISession(self.username, self.password)
            s.set_host(self.rally_host_name, self.ssl, self.admin_port)
            bucket_stats = s.api_get(f"/pools/default/buckets/{name}/stats").json()
            return bucket_stats
        except Exception as err:
            raise BucketStatsError(f"can not get bucket {name} stats: {err}")

    @retry()
    async def run_query(self, cluster: AsyncCluster, sql: str):
        contents = []
        result = cluster.query(sql)
        async for item in result:
            contents.append(item)
        return contents

    @retry(always_raise_list=(DocumentNotFoundException, ScopeNotFoundException, CollectionNotFoundException))
    async def get_doc(self, collection: AsyncCollection, doc_id: str):
        result = await collection.get(doc_id)
        return result.content_as[dict]

    @retry(always_raise_list=(ScopeNotFoundException, CollectionNotFoundException))
    async def put_doc(self, collection: AsyncCollection, doc_id: str, document: JSONType):
        result = await collection.upsert(doc_id, document)
        return result.cas

    async def index_by_query(self, sql: str):
        advisor = f"select advisor([\"{sql}\"])"
        cluster: AsyncCluster = await self.session_a()

        results = await self.run_query(cluster, advisor)

        current = results[0].get('$1', {}).get('current_used_indexes')
        if current:
            logger.debug("index already exists")
            return

        try:
            index_list = results[0]['$1']['recommended_indexes']
            for item in index_list:
                index_query = item['index']
                logger.debug(f"creating index: {index_query}")
                await self.run_query(cluster, index_query)
        except (KeyError, ValueError):
            logger.debug(f"can not get recommended index from query {advisor}")
            raise IndexInternalError(f"can not determine index for query")

    @retry()
    async def create_indexes(self, cluster: AsyncCluster, bucket: AsyncBucket, scope: AsyncScope, collection: AsyncCollection, fields: List[str], replica: int = 0):
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
                await qim.create_index(bucket.name, index_name, [field], index_options)
                await self.index_wait(cluster, bucket, scope, collection, index_name)
            await asyncio.sleep(0.5)
        except QueryIndexAlreadyExistsException:
            logger.debug(f"index already exists")
            pass

    @retry()
    async def create_primary_index(self, cluster: AsyncCluster, bucket: AsyncBucket, scope: AsyncScope, collection: AsyncCollection, replica: int = 0):
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
            await qim.create_primary_index(bucket.name, index_options)
            await self.index_wait_primary(cluster, bucket)
            await asyncio.sleep(0.5)
        except QueryIndexAlreadyExistsException:
            pass

    @retry(always_raise_list=(WatchQueryIndexTimeoutException,))
    async def index_wait(self, cluster: AsyncCluster, bucket: AsyncBucket, scope: AsyncScope, collection: AsyncCollection, index: str):
        watch_options = WatchQueryIndexOptions(
            collection_name=collection.name,
            scope_name=scope.name,
            timeout=timedelta(seconds=10)
        )
        qim = cluster.query_indexes()
        await qim.watch_indexes(bucket.name, [index], watch_options)

    @retry(always_raise_list=(WatchQueryIndexTimeoutException,))
    async def index_wait_primary(self, cluster: AsyncCluster, bucket: AsyncBucket):
        watch_options = WatchQueryIndexOptions(
            watch_primary=True,
            timeout=timedelta(seconds=10)
        )
        qim = cluster.query_indexes()
        await qim.watch_indexes(bucket.name, [], watch_options)

    @staticmethod
    async def is_scope(bucket: AsyncBucket, name: str):
        if name is None:
            raise TypeError("name can not be None")
        cm = bucket.collections()
        return next((s for s in await cm.get_all_scopes() if s.name == name), None)

    @staticmethod
    async def is_collection(bucket: AsyncBucket, scope: str, name: str):
        if name is None or scope is None:
            raise TypeError("name and scope can not be None")
        cm = bucket.collections()
        sm = next((s for s in await cm.get_all_scopes() if s.name == scope), None)
        return next((i for i in sm.collections if i.name == name), None)
