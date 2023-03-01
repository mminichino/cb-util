##
##

from .exceptions import (IndexNotReady, IndexNotFoundError, CollectionNameNotFound, IndexStatError, ClusterHealthCheckError)
from .retry import retry, retry_inline
from .cb_connect import CBConnect
from datetime import timedelta
import attr
import hashlib
from attr.validators import instance_of as io, optional
from typing import Protocol, Iterable
from couchbase.cluster import Cluster
from couchbase.options import QueryOptions
from couchbase.diagnostics import ServiceType, PingState
from couchbase.management.buckets import CreateBucketSettings, BucketType, StorageBackend
from couchbase.management.collections import CollectionSpec
from couchbase.exceptions import (QueryIndexNotFoundException, QueryIndexAlreadyExistsException, BucketAlreadyExistsException, BucketNotFoundException, BucketDoesNotExistException,
                                  WatchQueryIndexTimeoutException, ScopeAlreadyExistsException, CollectionAlreadyExistsException, CollectionNotFoundException)
from couchbase.management.queries import (CreateQueryIndexOptions, CreatePrimaryQueryIndexOptions, WatchQueryIndexOptions, DropPrimaryQueryIndexOptions, DropQueryIndexOptions)
from couchbase.management.options import CreateBucketOptions, CreateScopeOptions, CreateCollectionOptions, GetAllQueryIndexOptions
from couchbase.options import WaitUntilReadyOptions


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


class CBManager(CBConnect):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def create_bucket(self, name, quota: int = 256, replicas: int = 0):
        self.logger.debug(f"create_bucket: create bucket {name}")
        try:
            bm = self._cluster.buckets()
            bm.create_bucket(CreateBucketSettings(name=name,
                                                  bucket_type=BucketType.COUCHBASE,
                                                  storage_backend=StorageBackend.COUCHSTORE,
                                                  num_replicas=replicas,
                                                  ram_quota_mb=quota),
                             CreateBucketOptions(timeout=timedelta(seconds=25)))
        except BucketAlreadyExistsException:
            pass
        self.bucket(name)

    def drop_bucket(self, name):
        self.logger.debug(f"drop_bucket: drop bucket {name}")
        try:
            bm = self._cluster.buckets()
            bm.drop_bucket(name)
        except (BucketNotFoundException, BucketDoesNotExistException):
            pass

    def create_scope(self, name):
        self.logger.debug(f"create_scope: create scope {name}")
        try:
            if name != "_default":
                cm = self._bucket.collections()
                cm.create_scope(name, CreateScopeOptions(timeout=timedelta(seconds=25)))
        except ScopeAlreadyExistsException:
            pass
        self.scope(name)

    def create_collection(self, name):
        self.logger.debug(f"create_collection: create collection {name}")
        try:
            if name != "_default":
                collection_spec = CollectionSpec(name, scope_name=self._scope.name)
                cm = self._bucket.collections()
                cm.create_collection(collection_spec, CreateCollectionOptions(timeout=timedelta(seconds=25)))
                retry_inline(self.get_collection, cm, name)
        except CollectionAlreadyExistsException:
            pass
        self.collection(name)

    @staticmethod
    def get_scope(cm, scope_name):
        return next((s for s in cm.get_all_scopes() if s.name == scope_name), None)

    def get_collection(self, cm, collection_name):
        collection = None
        scope = self.get_scope(cm, self._scope.name)
        if scope:
            collection = next((c for c in scope.collections if c.name == collection_name), None)
        if not collection:
            raise CollectionNameNotFound(f"collection {collection_name} not found")
        else:
            return collection

    @retry()
    def drop_collection(self, name):
        self.logger.debug(f"drop_collection: drop collection {name}")
        try:
            collection_spec = CollectionSpec(name, scope_name=self._scope.name)
            cm = self._bucket.collections()
            cm.drop_collection(collection_spec)
        except CollectionNotFoundException:
            pass

    def wait_for_query_ready(self):
        cluster = Cluster.connect(self.cb_connect_string, self.cluster_options)
        cluster.wait_until_ready(timedelta(seconds=30), WaitUntilReadyOptions(service_types=[ServiceType.Query, ServiceType.Management]))

    @retry(factor=0.5)
    def wait_for_index_ready(self):
        value = []
        query_str = r"SELECT * FROM system:indexes;"
        cluster = Cluster.connect(self.cb_connect_string, self.cluster_options)
        result = cluster.query(query_str, QueryOptions(metrics=False, adhoc=True))
        for item in result:
            value.append(item)
        if len(value) >= 0:
            return True
        else:
            return False

    def cluster_health_check(self, output=False, restrict=True, extended=False):
        try:
            cluster = Cluster.connect(self.cb_connect_string, self.cluster_options)
            result = cluster.ping()
        except Exception as err:
            raise ClusterHealthCheckError("cluster unhealthy: {}".format(err))

        endpoint: ServiceType
        for endpoint, reports in result.endpoints.items():
            for report in reports:
                if restrict and endpoint != ServiceType.KeyValue:
                    continue
                report_string = " {0}: {1} took {2} {3}".format(
                    endpoint.value,
                    report.remote,
                    report.latency,
                    report.state.value)
                if output:
                    print(report_string)
                    continue
                if not report.state == PingState.OK:
                    print(f"{endpoint.value} service not ok: {report.state}")

        if output:
            print("Cluster Diagnostics:")
            diag_result = cluster.diagnostics()
            for endpoint, reports in diag_result.endpoints.items():
                for report in reports:
                    report_string = " {0}: {1} last activity {2} {3}".format(
                        endpoint.value,
                        report.remote,
                        report.last_activity,
                        report.state.value)
                    print(report_string)

        if extended:
            try:
                if 'n1ql' in self.cluster_services:
                    query = "select * from system:datastores ;"
                    result = cluster.query(query, QueryOptions(metrics=False, adhoc=True))
                    print(f"Datastore query ok: returned {len(result.rows())} records")
                if 'index' in self.cluster_services:
                    query = "select * from system:indexes ;"
                    result = cluster.query(query, QueryOptions(metrics=False, adhoc=True))
                    print(f"Index query ok: returned {len(result.rows())} records")
            except Exception as err:
                print(f"query service not ready: {err}")

    def cluster_schema_dump(self) -> dict:
        inventory = {
            "inventory": []
        }
        cluster = Cluster.connect(self.cb_connect_string, self.cluster_options)
        bm = cluster.buckets()
        qim = cluster.query_indexes()
        buckets = bm.get_all_buckets()
        for b in buckets:
            schema = {
                b.name: {
                    "buckets": [
                        {
                            "name": b.name,
                            "scopes": []
                        }
                    ]
                }
            }
            self.logger.debug(f"scanning bucket {b.name}")
            bucket = cluster.bucket(b.name)
            cm = bucket.collections()
            scopes = cm.get_all_scopes()
            for s in scopes:
                schema_scope = {
                    "name": s.name,
                    "collections": []
                }
                self.logger.debug(f"scanning scope {s.name}")
                collections = s.collections
                for c in collections:
                    self.logger.debug(f"scanning collection {c.name}")
                    primary_index = False
                    index_get_options = GetAllQueryIndexOptions(scope_name=s.name, collection_name=c.name)
                    indexes = qim.get_all_indexes(b.name, index_get_options)
                    index_names = list(map(lambda i: i.name, [index for index in indexes]))
                    index_keys_lists = list(map(lambda i: i.index_key, [index for index in indexes]))
                    index_keys = [item.strip('`') for sublist in index_keys_lists for item in sublist]
                    if '#primary' in index_names:
                        primary_index = True
                        index_names.remove('#primary')
                    schema_collection = {
                        "name": c.name,
                        "schema": {},
                        "idkey": "",
                        "primary_index": primary_index,
                        "override_count": False,
                        "indexes": index_keys
                    }
                    schema_scope['collections'].append(schema_collection)
                schema[b.name]["buckets"][0]["scopes"].append(schema_scope)
            inventory["inventory"].append(schema)
        return inventory

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
