##
##

from __future__ import annotations
import logging
from typing import Union, Dict, Any, List
from couchbase.cluster import Cluster
from couchbase.bucket import Bucket
from couchbase.scope import Scope
from couchbase.collection import Collection
from couchbase.exceptions import (QueryIndexNotFoundException, QueryIndexAlreadyExistsException, DocumentNotFoundException, BucketNotFoundException,
                                  ScopeNotFoundException, CollectionNotFoundException)
from cbcmgr.retry import retry
from cbcmgr.cb_session import CBSession, BucketMode

logger = logging.getLogger('cbutil.operation')
logger.addHandler(logging.NullHandler())
JSONType = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]


class CBOperation(CBSession):

    def __init__(self, *args, create: bool = False, quota: int = 256, replicas: int = 0, mode: BucketMode = BucketMode.DEFAULT, **kwargs):
        super().__init__(*args, **kwargs)
        logger.debug("begin operation class")
        self._cluster: Cluster = self.session()
        self._bucket: Bucket
        self._bucket_name = None
        self._scope: Scope
        self._scope_name = "_default"
        self._collection: Collection
        self._collection_name = "_default"
        self.create = create
        self.quota = quota
        self.replicas = replicas
        self.bucket_mode = mode

    def connect(self, keyspace: str):
        parts = keyspace.split('.')
        bucket = parts[0]
        scope = parts[1] if len(parts) > 1 else "_default"
        collection = parts[2] if len(parts) > 2 else "_default"
        logger.debug(f"connecting to {keyspace}")
        return self.bucket(bucket).scope(scope).collection(collection)

    def bucket(self, name: str):
        if name is None:
            raise TypeError("name can not be None")
        if self._cluster is None:
            raise ValueError("cluster not connected")
        try:
            self._bucket = self.get_bucket(self._cluster, name)
        except BucketNotFoundException:
            if self.create:
                self._create_bucket(self._cluster, name, self.quota, self.replicas, self.bucket_mode)
                return self.bucket(name)
            else:
                raise
        self._bucket_name = name
        return self

    def scope(self, name: str = "_default"):
        if self._bucket is None:
            raise ValueError("bucket not connected")
        try:
            self._scope = self.get_scope(self._bucket, name)
        except ScopeNotFoundException:
            if self.create:
                self._create_scope(self._bucket, name)
                return self.scope(name)
            else:
                raise
        self._scope_name = name
        return self

    def collection(self, name: str = "_default"):
        if self._scope is None:
            raise ValueError("scope not connected")
        try:
            self._collection = self.get_collection(self._bucket, self._scope, name)
        except CollectionNotFoundException:
            if self.create:
                self._create_collection(self._bucket, self._scope, name)
                return self.collection(name)
        self._collection_name = name
        return self

    @retry(always_raise_list=(DocumentNotFoundException, ScopeNotFoundException, CollectionNotFoundException))
    def get_doc(self, doc_id: str):
        result = self._collection.get(doc_id)
        return result.content_as[dict]

    @retry(always_raise_list=(ScopeNotFoundException, CollectionNotFoundException))
    def put_doc(self, doc_id: str, document: JSONType):
        result = self._collection.upsert(doc_id, document)
        return result.cas

    @retry(always_raise_list=(QueryIndexAlreadyExistsException, QueryIndexNotFoundException))
    def sql_query(self, sql: str):
        contents = []
        result = self._cluster.query(sql)
        for item in result:
            contents.append(item)
        return contents


class DBRead(CBOperation):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._result = None
        self.doc_id = None

    def get(self, doc_id: str):
        if doc_id is None:
            raise TypeError("name can not be None")
        self.doc_id = doc_id
        return self

    def execute(self):
        result = self.get_doc(self.doc_id)
        self._result = {self.doc_id: result}
        return self

    @property
    def result(self):
        return self._result


class DBWrite(CBOperation):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._result = None
        self.doc_id = None
        self.document: JSONType = None

    def put(self, doc_id: str, document: JSONType):
        if doc_id is None or document is None:
            raise TypeError("doc ID and document are required")
        self.doc_id = doc_id
        self.document = document
        return self

    def execute(self):
        result = self.put_doc(self.doc_id, self.document)
        self._result = {self.doc_id: result}
        return self

    @property
    def result(self):
        return self._result


class DBQuery(CBOperation):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._result = None
        self.sql = None

    def query(self, sql: str):
        if sql is None:
            raise TypeError("sql can not be None")
        self.sql = sql
        return self

    def execute(self):
        self._result = self.sql_query(self.sql)
        return self

    @property
    def result(self):
        return self._result
