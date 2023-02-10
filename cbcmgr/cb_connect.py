##
##

from .exceptions import (BucketWaitException, IndexNotReady, IndexNotFoundError, CollectionCountException,
                         CollectionNameNotFound, IndexExistsError, CollectionCountError, QueryArgumentsError,
                         QueryEmptyException, IndexStatError, BucketStatsError, ClusterNotConnected, BucketNotConnected,
                         ScopeNotConnected, CollectionSubdocUpsertError)
from .retry import retry
from .cb_session import CBSession
from .httpsessionmgr import APISession
from datetime import timedelta
import concurrent.futures
import attr
import hashlib
from attr.validators import instance_of as io, optional
from typing import Protocol, Iterable
from couchbase.cluster import Cluster
from couchbase.management.buckets import CreateBucketSettings, BucketType
from couchbase.management.collections import CollectionSpec
import couchbase.subdocument as SD
from couchbase.exceptions import (CouchbaseException, QueryIndexNotFoundException, DocumentNotFoundException,
                                  DocumentExistsException, QueryIndexAlreadyExistsException,
                                  BucketAlreadyExistsException, BucketNotFoundException,
                                  WatchQueryIndexTimeoutException, ScopeAlreadyExistsException,
                                  CollectionAlreadyExistsException, CollectionNotFoundException)
from couchbase.options import (QueryOptions, LockMode, ClusterOptions, TLSVerifyMode)
from couchbase.management.queries import (CreateQueryIndexOptions, CreatePrimaryQueryIndexOptions,
                                          WatchQueryIndexOptions, DropPrimaryQueryIndexOptions, DropQueryIndexOptions)


@attr.s
class CBQueryIndex(Protocol):
    name = attr.ib(validator=io(str))
    is_primary = attr.ib(validator=io(bool))
    state = attr.ib(validator=io(str))
    namespace = attr.ib(validator=io(str))
    keyspace = attr.ib(validator=io(str))
    index_key = attr.ib(validator=io(Iterable))
    condition = attr.ib(validator=io(str))
    bucket_name = attr.ib(validator=optional(io(str)))
    scope_name = attr.ib(validator=optional(io(str)))
    collection_name = attr.ib(validator=optional(io(str)))
    partition = attr.ib(validator=optional(validator=io(str)))

    @classmethod
    def from_server(cls, json_data):
        return cls(json_data.get("name"),
                   bool(json_data.get("is_primary")),
                   json_data.get("state"),
                   json_data.get("keyspace_id"),
                   json_data.get("namespace_id"),
                   json_data.get("index_key", []),
                   json_data.get("condition", ""),
                   json_data.get("bucket_id", json_data.get("keyspace_id", "")),
                   json_data.get("scope_id", ""),
                   json_data.get("keyspace_id", ""),
                   json_data.get("partition", None)
                   )


class CBConnect(CBSession):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_reachable()
        self.cluster_options = ClusterOptions(self.auth,
                                              timeout_options=self.timeouts,
                                              tls_verify=TLSVerifyMode.NO_VERIFY,
                                              lockmode=LockMode.WAIT)
        if self.use_external_network:
            self.cluster_options.update(network="external")
        else:
            self.cluster_options.update(network="default")

    @retry(factor=0.5)
    def connect(self):
        self.logger.debug(f"connect: connect string {self.cb_connect_string}")
        cluster = Cluster.connect(self.cb_connect_string, self.cluster_options)
        self._cluster = cluster
        return self

    @retry()
    def bucket(self, name):
        self.logger.debug(f"bucket: connecting bucket {name}")
        if self._cluster:
            self._bucket = self._cluster.bucket(name)
        else:
            raise ClusterNotConnected("no cluster connected")
        return self

    @retry()
    def scope(self, name="_default"):
        if self._bucket:
            self.logger.debug(f"scope: connecting scope {name}")
            self._scope = self._bucket.scope(name)
            self._scope_name = name
        else:
            raise BucketNotConnected("bucket not connected")
        return self

    def collection(self, name="_default"):
        if self._scope:
            self.logger.debug(f"collection: connecting collection {name}")
            self._collection = self._scope.collection(name)
            self._collection_name = name
        else:
            raise ScopeNotConnected("scope not connected")
        return self

    @retry(factor=0.5)
    def bucket_wait(self, bucket: str, count: int = 0):
        try:
            bucket_stats = self.bucket_stats(bucket)
            if bucket_stats['itemCount'] < count:
                raise BucketWaitException(f"item count {bucket_stats['itemCount']} less than {count}")
        except Exception as err:
            raise BucketWaitException(f"bucket_wait: error: {err}")

    @retry()
    def bucket_stats(self, bucket):
        try:
            hostname = self.rally_host_name
            s = APISession(self.username, self.password)
            s.set_host(hostname, self.ssl, self.admin_port)
            results = s.api_get(f"/pools/default/buckets/{bucket}")
            basic_stats = results.json()['basicStats']
            return basic_stats
        except Exception as err:
            raise BucketStatsError(f"can not get bucket {bucket} stats: {err}")

    @retry()
    def create_bucket(self, name, quota=256):
        self.logger.debug(f"create_bucket: create bucket {name}")
        try:
            bm = self._cluster.buckets()
            bm.create_bucket(CreateBucketSettings(name=name,
                                                  bucket_type=BucketType.COUCHBASE,
                                                  ram_quota_mb=quota))
        except BucketAlreadyExistsException:
            pass
        return self.bucket(name)

    @retry()
    def drop_bucket(self, name):
        self.logger.debug(f"drop_bucket: drop bucket {name}")
        try:
            bm = self._cluster.buckets()
            bm.drop_bucket(name)
        except BucketNotFoundException:
            pass

    @retry()
    def create_scope(self, name):
        self.logger.debug(f"create_scope: create scope {name}")
        try:
            if name != "_default":
                cm = self._bucket.collections()
                cm.create_scope(name)
        except ScopeAlreadyExistsException:
            pass
        return self.scope(name)

    @retry()
    def create_collection(self, name):
        self.logger.debug(f"create_collection: create collection {name}")
        try:
            if name != "_default":
                collection_spec = CollectionSpec(name, scope_name=self._scope.name)
                cm = self._bucket.collections()
                cm.create_collection(collection_spec)
        except CollectionAlreadyExistsException:
            pass
        return self.collection(name)

    @retry()
    def drop_collection(self, name):
        self.logger.debug(f"drop_collection: drop collection {name}")
        try:
            collection_spec = CollectionSpec(name, scope_name=self._scope.name)
            cm = self._bucket.collections()
            cm.drop_collection(collection_spec)
        except CollectionNotFoundException:
            pass

    @retry()
    def collection_count(self, expect_count: int = 0) -> int:
        try:
            query = 'select count(*) as count from ' + self.keyspace + ';'
            result = self.cb_query(sql=query)
            count: int = int(result[0]['count'])
            if expect_count > 0:
                if count < expect_count:
                    raise CollectionCountException(f"expect count {expect_count} but current count is {count}")
            return count
        except Exception as err:
            self.logger.error(f"collection_count: error occurred: {err}")
            raise CollectionCountError(f"can not get item count for {self.keyspace}: {err}")

    @retry()
    def cb_get(self, key):
        try:
            document_id = self.construct_key(key)
            result = self._collection.get(document_id)
            self.logger.debug(f"cb_get: {document_id}: cas {result.cas}")
            return result.content_as[dict]
        except DocumentNotFoundException:
            return None

    @retry()
    def cb_upsert(self, key, document):
        try:
            document_id = self.construct_key(key)
            result = self._collection.upsert(document_id, document)
            self.logger.debug(f"cb_upsert: {document_id}: cas {result.cas}")
            return result
        except DocumentExistsException:
            return None

    @retry()
    def cb_subdoc_upsert(self, key, field, value):
        document_id = self.construct_key(key)
        result = self._collection.mutate_in(document_id, [SD.upsert(field, value)])
        self.logger.debug(f"cb_subdoc_upsert: {document_id}: cas {result.cas}")
        return result.content_as[dict]

    @retry()
    def cb_subdoc_multi_upsert(self, key_list, field, value_list):
        tasks = set()
        executor = concurrent.futures.ThreadPoolExecutor()
        for n in range(len(key_list)):
            tasks.add(executor.submit(self.cb_subdoc_upsert, key_list[n], field, value_list[n]))
        while tasks:
            done, tasks = concurrent.futures.wait(tasks, return_when=concurrent.futures.FIRST_COMPLETED)
            for task in done:
                try:
                    result = task.result()
                except Exception as err:
                    raise CollectionSubdocUpsertError(f"multi upsert error: {err}")

    def query_sql_constructor(self, field=None, where=None, value=None, sql=None):
        if not where and not sql and field:
            query = "SELECT " + field + " FROM " + self.keyspace + ";"
        elif not sql and field:
            query = "SELECT " + field + " FROM " + self.keyspace + " WHERE " + where + " = \"" + str(value) + "\";"
        elif sql:
            query = sql
        else:
            raise QueryArgumentsError("query: either field or sql argument is required")
        return query

    @retry(
        always_raise_list=(CollectionNameNotFound, QueryArgumentsError, IndexExistsError, QueryIndexNotFoundException))
    def cb_query(self, field=None, where=None, value=None, sql=None, empty_retry=False):
        query = self.query_sql_constructor(field, where, value, sql)
        contents = []
        try:
            self.logger.debug(f"cb_query: running query: {query}")
            result = self._cluster.query(query, QueryOptions(metrics=False, adhoc=True))
            for item in result:
                contents.append(item)
            if empty_retry:
                if len(contents) == 0:
                    raise QueryEmptyException(f"query did not return any results")
            return contents
        except QueryIndexAlreadyExistsException:
            pass
        except QueryIndexNotFoundException:
            pass
        except CouchbaseException:
            raise

    def index_name(self, fields: list[str]):
        hash_string = ','.join(fields)
        name_part = hashlib.shake_256(hash_string.encode()).hexdigest(3)

        if self._collection_name != '_default':
            name = self._collection_name + '_' + name_part + '_ix'
        else:
            name = self._bucket.name + '_' + name_part + '_ix'

        return name

    @retry()
    def cb_create_primary_index(self, replica: int = 0, timeout: int = 480):
        if self._collection.name != '_default':
            index_options = CreatePrimaryQueryIndexOptions(deferred=False,
                                                           timeout=timedelta(seconds=timeout),
                                                           num_replicas=replica,
                                                           collection_name=self._collection.name,
                                                           scope_name=self._scope.name)
        else:
            index_options = CreatePrimaryQueryIndexOptions(deferred=False,
                                                           timeout=timedelta(seconds=timeout),
                                                           num_replicas=replica)
        self.logger.debug(
            f"cb_create_primary_index: creating primary index on {self._collection.name}")
        try:
            qim = self._cluster.query_indexes()
            qim.create_primary_index(self._bucket.name, index_options)
        except QueryIndexAlreadyExistsException:
            pass

    @retry()
    def cb_create_index(self, fields: list[str], replica: int = 0, timeout: int = 480):
        if self._collection.name != '_default':
            index_options = CreateQueryIndexOptions(deferred=False,
                                                    timeout=timedelta(seconds=timeout),
                                                    num_replicas=replica,
                                                    collection_name=self._collection.name,
                                                    scope_name=self._scope.name)
        else:
            index_options = CreateQueryIndexOptions(deferred=False,
                                                    timeout=timedelta(seconds=timeout),
                                                    num_replicas=replica)
        try:
            index_name = self.index_name(fields)
            qim = self._cluster.query_indexes()
            self.logger.debug(
                f"creating index {index_name} on {','.join(fields)} for {self.keyspace}")
            qim.create_index(self._bucket.name, index_name, fields, index_options)
            return index_name
        except QueryIndexAlreadyExistsException:
            pass

    @retry()
    def cb_drop_primary_index(self, timeout: int = 120):
        if self._collection_name != '_default':
            index_options = DropPrimaryQueryIndexOptions(timeout=timedelta(seconds=timeout),
                                                         collection_name=self._collection.name,
                                                         scope_name=self._scope.name)
        else:
            index_options = DropPrimaryQueryIndexOptions(timeout=timedelta(seconds=timeout))
        self.logger.debug(f"cb_drop_primary_index: dropping primary index on {self.collection_name}")
        try:
            qim = self._cluster.query_indexes()
            qim.drop_primary_index(self._bucket.name, index_options)
        except QueryIndexNotFoundException:
            pass

    @retry()
    def cb_drop_index(self, name: str, timeout: int = 120):
        if self._collection_name != '_default':
            index_options = DropQueryIndexOptions(timeout=timedelta(seconds=timeout),
                                                  collection_name=self._collection.name,
                                                  scope_name=self._scope.name)
        else:
            index_options = DropQueryIndexOptions(timeout=timedelta(seconds=timeout))
        try:
            self.logger.debug(f"cb_drop_index: drop index {name}")
            qim = self._cluster.query_indexes()
            qim.drop_index(self._bucket.name, name, index_options)
        except QueryIndexNotFoundException:
            pass

    @retry()
    def index_list_all(self):
        all_list = []
        query_str = r"SELECT * FROM system:indexes ;"
        results = self.cb_query(sql=query_str)

        for row in results:
            for key, value in row.items():
                entry = CBQueryIndex.from_server(value)
                all_list.append(entry)

        return all_list

    def is_index(self, index_name: str = None):
        if not index_name:
            index_name = '#primary'
        try:
            index_list = self.index_list_all()
            for item in index_list:
                if index_name == '#primary':
                    if (item.collection_name == self.collection_name or item.bucket_name == self.collection_name) \
                            and item.name == '#primary':
                        return True
                elif item.name == index_name:
                    return True
        except Exception as err:
            raise IndexStatError("Could not get index status: {}".format(err))

        return False

    @retry(factor=0.5, allow_list=(IndexNotReady,))
    def index_wait(self, index_name: str = None):
        record_count = self.collection_count()
        try:
            self.index_check(index_name=index_name, check_count=record_count)
        except Exception:
            raise IndexNotReady(f"index_wait: index not ready")

    def get_index_key(self, index_name: str = None):
        if not index_name:
            index_name = '#primary'
        doc_key_field = 'meta().id'
        index_list = self.index_list_all()

        for item in index_list:
            if item.name == index_name and (
                    item.collection_name == self.collection_name or item.bucket_name == self.collection_name):
                if len(list(item.index_key)) == 0:
                    return doc_key_field
                else:
                    return list(item.index_key)[0]

        raise IndexNotFoundError(f"index {index_name} not found")

    def index_check(self, index_name: str = None, check_count: int = 0):
        try:
            query_field = self.get_index_key(index_name)
        except Exception:
            raise

        query_text = f"SELECT {query_field} FROM {self.keyspace} WHERE TOSTRING({query_field}) LIKE \"%\" ;"
        result = self.cb_query(sql=query_text)

        if check_count >= len(result):
            return True
        else:
            raise IndexNotReady(
                f"index_check: name: {index_name} count {check_count} len {len(result)}: index not ready")

    @retry(always_raise_list=(WatchQueryIndexTimeoutException,))
    def index_online(self, name=None, primary=False, timeout=480):
        if primary:
            indexes = []
            watch_options = WatchQueryIndexOptions(timeout=timedelta(seconds=timeout), watch_primary=True)
        else:
            indexes = [name]
            watch_options = WatchQueryIndexOptions(timeout=timedelta(seconds=timeout))
        try:
            qim = self._cluster.query_indexes()
            qim.watch_indexes(self._bucket.name,
                              indexes,
                              watch_options)
        except QueryIndexNotFoundException:
            raise IndexNotReady("index does not exist")
        except WatchQueryIndexTimeoutException:
            raise IndexNotReady(f"Indexes not build within {timeout} seconds...")

    @retry(factor=0.5, allow_list=(IndexNotReady,))
    def index_list(self):
        return_list = {}
        try:
            index_list = self.index_list_all()
            for item in index_list:
                if item.collection_name == self.collection_name or item.bucket_name == self.collection_name:
                    return_list[item.name] = item.state
            return return_list
        except Exception as err:
            raise IndexNotReady(f"index_list: bucket {self._bucket.name} error: {err}")

    @retry(factor=0.5, allow_list=(IndexNotReady,))
    def delete_wait(self, index_name: str = None):
        if self.is_index(index_name=index_name):
            raise IndexNotReady(f"delete_wait: index still exists")
