##
##

from __future__ import annotations
import logging
from typing import Union, Dict, Any, List
from acouchbase.cluster import AsyncCluster
from acouchbase.bucket import AsyncBucket
from acouchbase.scope import AsyncScope
from acouchbase.collection import AsyncCollection
from couchbase.exceptions import (BucketDoesNotExistException, BucketNotFoundException, ScopeNotFoundException, CollectionNotFoundException)
from cbcmgr.cb_session import BucketMode
from cbcmgr.cb_connect_lite_a import CBConnectLiteAsync
from cbcmgr.cb_operation_s import Operation

logger = logging.getLogger('cbutil.operation')
logger.addHandler(logging.NullHandler())
JSONType = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]


class CBOperationAsync(CBConnectLiteAsync):

    def __init__(self, *args, create: bool = False, quota: int = 256, replicas: int = 0, mode: BucketMode = BucketMode.DEFAULT, **kwargs):
        super().__init__(*args, **kwargs)
        logger.debug("begin operation class")
        self._cluster: AsyncCluster
        self._bucket: AsyncBucket
        self._bucket_name = None
        self._bucket_connected = False
        self._scope: AsyncScope
        self._scope_name = "_default"
        self._scope_connected = False
        self._collection: AsyncCollection
        self._collection_name = "_default"
        self._collection_connected = False
        self.create = create
        self.quota = quota
        self.replicas = replicas
        self.bucket_mode = mode

    class DBRead:

        def __init__(self, opm: CBOperationAsync):
            self.opm = opm
            self._result = None
            self.doc_id = None

        def prep(self, doc_id: str):
            if doc_id is None:
                raise TypeError("name can not be None")
            self.doc_id = doc_id
            return self

        async def execute(self):
            result = await self.opm.get_doc(self.opm.collection, self.doc_id)
            self._result = {self.doc_id: result}
            return self._result

        @property
        def result(self):
            return self._result

    class DBWrite:

        def __init__(self, opm: CBOperationAsync):
            self.opm = opm
            self._result = None
            self.doc_id = None
            self.document: JSONType = None

        def prep(self, doc_id: str, document: JSONType):
            if doc_id is None or document is None:
                raise TypeError("doc ID and document are required")
            self.doc_id = doc_id
            self.document = document
            return self

        async def execute(self):
            result = await self.opm.put_doc(self.opm.collection, self.doc_id, self.document)
            self._result = {self.doc_id: result}
            return self._result

        @property
        def result(self):
            return self._result

    class DBQuery:

        def __init__(self, opm: CBOperationAsync):
            self.opm = opm
            self._result = None
            self.sql = None

        def prep(self, sql: str):
            if sql is None:
                raise TypeError("sql can not be None")
            self.sql = sql
            return self

        async def execute(self):
            self._result = await self.opm.run_query(self.opm.cluster, self.sql)
            return self._result

        @property
        def result(self):
            return self._result

    async def init(self):
        self._cluster = await self.session_a()
        return self

    async def connect(self, keyspace: str):
        parts = keyspace.split('.')
        bucket = parts[0]
        scope = parts[1] if len(parts) > 1 else "_default"
        collection = parts[2] if len(parts) > 2 else "_default"
        logger.debug(f"connecting to {keyspace}")
        await self._bucket_(bucket)
        await self._scope_(scope)
        await self._collection_(collection)
        return self

    async def reconnect(self):
        logger.debug("reconnecting to cluster")
        await self._cluster.close()
        self._cluster = await self.session_a()
        if self._bucket_connected:
            self._bucket = await self.get_bucket(self._cluster, self._bucket_name)
            if self._scope_connected:
                self._scope = await self.get_scope(self._bucket, self._scope_name)
                if self._collection_connected:
                    self._collection = await self.get_collection(self._bucket, self._scope, self._collection_name)

    async def _bucket_(self, name: str):
        if name is None:
            raise TypeError("name can not be None")
        if self._cluster is None:
            raise ValueError("cluster not connected")
        try:
            self._bucket = await self.get_bucket(self._cluster, name)
        except BucketNotFoundException:
            if self.create:
                await self.create_bucket(self._cluster, name, self.quota, self.replicas, self.bucket_mode)
                return await self._bucket_(name)
            else:
                raise
        self._bucket_name = name
        self._bucket_connected = True
        return self._bucket

    async def _scope_(self, name: str = "_default"):
        if self._bucket is None:
            raise ValueError("bucket not connected")
        try:
            self._scope = await self.get_scope(self._bucket, name)
        except ScopeNotFoundException:
            if self.create:
                await self.create_scope(self._bucket, name)
                return await self._scope_(name)
            else:
                raise
        self._scope_name = name
        self._scope_connected = True
        return self._scope

    async def _collection_(self, name: str = "_default"):
        if self._scope is None:
            raise ValueError("scope not connected")
        try:
            self._collection = await self.get_collection(self._bucket, self._scope, name)
        except CollectionNotFoundException:
            if self.create:
                await self.create_collection(self._bucket, self._scope, name)
                await self.reconnect()
                return await self._collection_(name)
        self._collection_name = name
        self._collection_connected = True
        return self._collection

    async def cleanup(self):
        if not self._cluster or not self._cluster.connected:
            return
        logger.debug(f"cleanup: drop bucket {self._bucket.name}")
        try:
            bm = self._cluster.buckets()
            await bm.drop_bucket(self._bucket.name)
        except (BucketNotFoundException, BucketDoesNotExistException):
            pass

    async def get_count(self) -> int:
        return await self.collection_count(self._cluster, self.get_keyspace)

    @property
    def cluster(self):
        return self._cluster

    @property
    def bucket(self):
        return self._bucket

    @property
    def scope(self):
        return self._scope

    @property
    def collection(self):
        return self._collection

    @property
    def get_keyspace(self):
        return f"{self._bucket_name}.{self._scope_name}.{self._collection_name}"

    def get_operator(self, op: Operation):
        if op == Operation.READ:
            return self.DBRead(self)
        elif op == Operation.WRITE:
            return self.DBWrite(self)
        elif op == Operation.QUERY:
            return self.DBQuery(self)
        else:
            raise ValueError(f"unknown operation {op.name}")
