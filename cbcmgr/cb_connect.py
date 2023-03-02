##
##

from .exceptions import (CollectionNameNotFound, IndexExistsError, QueryArgumentsError, QueryEmptyException, ClusterNotConnected, BucketNotConnected,
                         ScopeNotConnected, CollectionSubdocUpsertError, BucketWaitException, BucketStatsError, CollectionCountException, CollectionCountError)
from .retry import retry, retry_inline
from .cb_session import CBSession
from .httpsessionmgr import APISession
from datetime import timedelta
from typing import Union, Dict, Any, List
import concurrent.futures
from couchbase.cluster import Cluster
import couchbase.subdocument as SD
from couchbase.exceptions import (CouchbaseException, QueryIndexNotFoundException, DocumentNotFoundException, DocumentExistsException, QueryIndexAlreadyExistsException)
from couchbase.options import (QueryOptions, LockMode, ClusterOptions, TLSVerifyMode, WaitUntilReadyOptions)
from couchbase.management.options import GetAllQueryIndexOptions
from couchbase.management.queries import CreatePrimaryQueryIndexOptions, DropPrimaryQueryIndexOptions
from couchbase.diagnostics import ServiceType

JSONType = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]


class CBConnect(CBSession):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_reachable()
        self.check_cluster()
        self.cluster_options = ClusterOptions(self.auth,
                                              timeout_options=self.timeouts,
                                              tls_verify=TLSVerifyMode.NO_VERIFY,
                                              lockmode=LockMode.WAIT)
        if self.use_external_network:
            self.cluster_options.update(network="external")
        else:
            self.cluster_options.update(network="default")

    def connect(self, bucket: str = None, scope: str = "_default", collection: str = "_default"):
        self.logger.debug(f"connect: connect string {self.cb_connect_string}")
        self._cluster = Cluster.connect(self.cb_connect_string, self.cluster_options)
        self._cluster.wait_until_ready(timedelta(seconds=4), WaitUntilReadyOptions(service_types=[ServiceType.KeyValue, ServiceType.Management]))
        if bucket:
            self.bucket(bucket)
            self.scope(scope)
            self.collection(collection)
        return self

    def bucket(self, name: str):
        self.logger.debug(f"bucket: connecting bucket {name}")
        if self._cluster:
            self._bucket = retry_inline(self._cluster.bucket, name)
        else:
            raise ClusterNotConnected("no cluster connected")

    def scope(self, name: str = "_default"):
        if self._bucket:
            self.logger.debug(f"scope: connecting scope {name}")
            self._cluster.wait_until_ready(timedelta(seconds=4), WaitUntilReadyOptions(service_types=[ServiceType.KeyValue]))
            self._scope = self._bucket.scope(name)
            self._scope_name = name
        else:
            raise BucketNotConnected("bucket not connected")

    def collection(self, name: str = "_default"):
        if self._scope:
            self.logger.debug(f"collection: connecting collection {name}")
            self._cluster.wait_until_ready(timedelta(seconds=4), WaitUntilReadyOptions(service_types=[ServiceType.KeyValue]))
            self._collection = self._scope.collection(name)
            self._collection_name = name
        else:
            raise ScopeNotConnected("scope not connected")

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
            raise CollectionCountError(f"can not get item count for {self.keyspace}: {err}")

    @retry()
    def bucket_stats(self, bucket: str):
        try:
            hostname = self.rally_host_name
            s = APISession(self.username, self.password)
            s.set_host(hostname, self.ssl, self.admin_port)
            results = s.api_get(f"/pools/default/buckets/{bucket}")
            basic_stats = results.json()['basicStats']
            return basic_stats
        except Exception as err:
            raise BucketStatsError(f"can not get bucket {bucket} stats: {err}")

    @retry(factor=0.5)
    def bucket_wait(self, bucket: str, count: int = 0):
        try:
            bucket_stats = self.bucket_stats(bucket)
            if bucket_stats['itemCount'] < count:
                raise BucketWaitException(f"item count {bucket_stats['itemCount']} less than {count}")
        except Exception as err:
            raise BucketWaitException(f"bucket_wait: error: {err}")

    def has_primary_index(self, create: bool = False, replica: int = 0, timeout: int = 480):
        qim = self._cluster.query_indexes()
        index_get_options = GetAllQueryIndexOptions(scope_name=self._scope_name, collection_name=self._collection_name)
        indexes = qim.get_all_indexes(self._bucket.name, index_get_options)
        index_names = list(map(lambda i: i.name, [index for index in indexes]))
        if '#primary' in index_names:
            return True
        else:
            if create:
                index_options = CreatePrimaryQueryIndexOptions(deferred=False, timeout=timedelta(seconds=timeout), num_replicas=replica,
                                                               collection_name=self._collection.name, scope_name=self._scope.name)
                try:
                    qim.create_primary_index(self._bucket.name, index_options)
                except QueryIndexAlreadyExistsException:
                    pass
                return True
            else:
                return False

    def revert_primary_index(self, timeout: int = 480):
        qim = self._cluster.query_indexes()
        try:
            index_options = DropPrimaryQueryIndexOptions(timeout=timedelta(seconds=timeout), collection_name=self._collection.name, scope_name=self._scope.name)
            qim.drop_primary_index(self._bucket.name, index_options)
        except QueryIndexNotFoundException:
            pass

    @retry()
    def cb_get(self, key: Union[int, str]):
        try:
            document_id = self.construct_key(key)
            result = self._collection.get(document_id)
            self.logger.debug(f"cb_get: {document_id}: cas {result.cas}")
            return result.content_as[dict]
        except DocumentNotFoundException:
            return None

    @retry()
    def cb_upsert(self, key: Union[int, str], document: JSONType):
        try:
            self.logger.debug(f"cb_upsert: key {key}")
            document_id = self.construct_key(key)
            result = self._collection.upsert(document_id, document)
            self.logger.debug(f"cb_upsert: {document_id}: cas {result.cas}")
            return result
        except DocumentExistsException:
            return None

    @retry()
    def cb_subdoc_upsert(self, key: Union[int, str], field: str, value: JSONType):
        document_id = self.construct_key(key)
        result = self._collection.mutate_in(document_id, [SD.upsert(field, value)])
        self.logger.debug(f"cb_subdoc_upsert: {document_id}: cas {result.cas}")
        return result.content_as[dict]

    @retry()
    def cb_subdoc_multi_upsert(self, key_list: list, field: str, value_list: list):
        tasks = set()
        executor = concurrent.futures.ThreadPoolExecutor()
        for n in range(len(key_list)):
            tasks.add(executor.submit(self.cb_subdoc_upsert, key_list[n], field, value_list[n]))
        while tasks:
            done, tasks = concurrent.futures.wait(tasks, return_when=concurrent.futures.FIRST_COMPLETED)
            for task in done:
                try:
                    task.result()
                except Exception as err:
                    raise CollectionSubdocUpsertError(f"multi upsert error: {err}")

    def query_sql_constructor(self, field: str = None, where: str = None, value: str = None, sql: str = None):
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
    def cb_query(self, field: str = None, where: str = None, value: str = None, sql: str = None, empty_retry: bool = False):
        query = self.query_sql_constructor(field, where, value, sql)
        contents = []
        try:
            self._cluster.wait_until_ready(timedelta(seconds=4), WaitUntilReadyOptions(service_types=[ServiceType.Query]))
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
