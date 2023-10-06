##
##

from __future__ import annotations
from .cb_bucket import Bucket
from .cb_index import CBQueryIndex
from .exceptions import (IndexNotReady, IndexNotFoundError, CollectionNameNotFound, IndexStatError, ClusterHealthCheckError, PathMapUpsertError, CollectionUpsertError,
                         ScopeCreateException, BucketCreateException, CollectionCreateException)
from .retry import retry, retry_inline
from .cb_connect import CBConnect
from .util import r_getattr, omit_path, copy_path
from .config import UpsertMapConfig, MapUpsertType
from .cb_capella import Capella
from datetime import timedelta
import attr
import hashlib
import logging
import json
import xmltodict
import concurrent.futures
from typing import Optional, Any
from couchbase.cluster import Cluster
from couchbase.options import QueryOptions
from couchbase.diagnostics import ServiceType, PingState
from couchbase.management.buckets import CreateBucketSettings, BucketSettings
from couchbase.management.collections import CollectionSpec
from couchbase.exceptions import (QueryIndexNotFoundException, QueryIndexAlreadyExistsException, BucketAlreadyExistsException, BucketNotFoundException, BucketDoesNotExistException,
                                  WatchQueryIndexTimeoutException, ScopeAlreadyExistsException, CollectionAlreadyExistsException, CollectionNotFoundException)
from couchbase.management.queries import (CreateQueryIndexOptions, CreatePrimaryQueryIndexOptions, WatchQueryIndexOptions, DropPrimaryQueryIndexOptions, DropQueryIndexOptions)
from couchbase.management.options import CreateBucketOptions, CreateScopeOptions, CreateCollectionOptions, GetAllQueryIndexOptions
from couchbase.options import WaitUntilReadyOptions, UpsertOptions

logger = logging.getLogger('cbutil.manager')
logger.addHandler(logging.NullHandler())


class CBManager(CBConnect):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def connect_cluster(self) -> CBManager:
        self._cluster = self.session()
        return self

    def create_bucket(self, bucket: Bucket):
        self._cluster.wait_until_ready(timedelta(seconds=5), WaitUntilReadyOptions(service_types=[ServiceType.KeyValue]))

        result = self.get_bucket(bucket.name)
        if result:
            logger.debug(f"create_bucket: bucket {bucket.name} already exists")
        else:
            logger.debug(f"create_bucket: create bucket {bucket.name}")
            if self.capella_project and self.capella_db:
                project = Capella().get_project(self.capella_project)
                if not project:
                    raise BucketCreateException(f"Can not lookup Capella project {self.capella_project}")
                project_id = project.get('id')
                cluster = Capella(project_id=project_id).get_cluster(self.capella_db)
                if not cluster:
                    raise BucketCreateException(f"Can not find Capella database {self.capella_db}")
                cluster_id = cluster.get('id')
                logger.debug(f"Creating Capella bucket {bucket.name} in project {project_id} database {cluster_id}")
                Capella(project_id=project_id).add_bucket(cluster_id, bucket)
            else:
                try:
                    bm = self._cluster.buckets()
                    # noinspection PyTypeChecker
                    bm.create_bucket(CreateBucketSettings(**attr.asdict(bucket)), CreateBucketOptions(timeout=timedelta(seconds=25)))
                except BucketAlreadyExistsException:
                    pass

        self.bucket(bucket.name)

    def drop_bucket(self, name):
        logger.debug(f"drop_bucket: drop bucket {name}")
        try:
            bm = self._cluster.buckets()
            bm.drop_bucket(name)
        except (BucketNotFoundException, BucketDoesNotExistException):
            pass

    def bucket_list_all(self):
        bm = self._cluster.buckets()
        return bm.get_all_buckets()

    def scope_list_all(self):
        cm = self._bucket.collections()
        return cm.get_all_scopes()

    @staticmethod
    def collection_list_all(scope):
        return scope.collections

    def create_scope(self, name):
        if not name:
            raise ScopeCreateException(f"scope name can not be null")
        self._cluster.wait_until_ready(timedelta(seconds=5), WaitUntilReadyOptions(service_types=[ServiceType.KeyValue]))
        logger.debug(f"create_scope: create scope {name}")
        try:
            if name != "_default":
                cm = self._bucket.collections()
                cm.create_scope(name, CreateScopeOptions(timeout=timedelta(seconds=25)))
        except ScopeAlreadyExistsException:
            pass
        self.scope(name)

    def create_collection(self, name, max_ttl=0):
        if not name:
            raise CollectionCreateException(f"collection name can not be null")
        self._cluster.wait_until_ready(timedelta(seconds=5), WaitUntilReadyOptions(service_types=[ServiceType.KeyValue]))
        logger.debug(f"create_collection: create collection {name}")
        try:
            if name != "_default":
                collection_spec = CollectionSpec(name, scope_name=self._scope.name, max_ttl=timedelta(max_ttl))
                cm = self._bucket.collections()
                cm.create_collection(collection_spec, CreateCollectionOptions(timeout=timedelta(seconds=25)))
                retry_inline(self.get_collection, cm, name)
        except CollectionAlreadyExistsException:
            pass
        self.collection(name)

    def get_bucket(self, name: str) -> Optional[BucketSettings]:
        try:
            bm = self._cluster.buckets()
            return bm.get_bucket(name)
        except BucketDoesNotExistException:
            return None

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
        logger.debug(f"drop_collection: drop collection {name}")
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
            logger.debug(f"scanning bucket {b.name}")
            bucket = cluster.bucket(b.name)
            cm = bucket.collections()
            scopes = cm.get_all_scopes()
            for s in scopes:
                schema_scope = {
                    "name": s.name,
                    "collections": []
                }
                logger.debug(f"scanning scope {s.name}")
                collections = s.collections
                for c in collections:
                    logger.debug(f"scanning collection {c.name}")
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
        logger.debug(
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
            logger.debug(
                f"creating index {index_name} on {','.join(fields)} for {self.keyspace}")
            qim.create_index(self._bucket.name, index_name, fields, index_options)
            return index_name
        except QueryIndexAlreadyExistsException:
            pass

    @retry()
    def cb_index_create(self, index: CBQueryIndex, timeout: int = 480):
        if index.is_primary:
            index_options = CreatePrimaryQueryIndexOptions()
        else:
            index_options = CreateQueryIndexOptions()

        index_options.update(deferred=True)
        index_options.update(timeout=timedelta(seconds=timeout))
        index_options.update(num_replicas=index.num_replica)
        index_options.update(ignore_if_exists=True)
        if index.bucket_id:
            index_options.update(scope_name=index.scope_id)
            index_options.update(collection_name=index.keyspace_id)
        if index.condition:
            index_options.update(condition=index.condition)

        if index.bucket_id:
            bucket_name = index.bucket_id
        else:
            bucket_name = index.keyspace_id

        qim = self._cluster.query_indexes()

        if index.is_primary:
            qim.create_primary_index(bucket_name, index_options)
        else:
            qim.create_index(bucket_name, index.name, index.index_key, index_options)

    @retry()
    def cb_drop_primary_index(self, timeout: int = 120):
        if self._collection_name != '_default':
            index_options = DropPrimaryQueryIndexOptions(timeout=timedelta(seconds=timeout),
                                                         collection_name=self._collection.name,
                                                         scope_name=self._scope.name)
        else:
            index_options = DropPrimaryQueryIndexOptions(timeout=timedelta(seconds=timeout))
        logger.debug(f"cb_drop_primary_index: dropping primary index on {self.collection_name}")
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
            logger.debug(f"cb_drop_index: drop index {name}")
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
                entry = CBQueryIndex.from_dict(value)
                all_list.append(entry)

        return all_list

    def is_index(self, index_name: str = None):
        if not index_name:
            index_name = '#primary'
        try:
            index_list = self.index_list_all()
            for item in index_list:
                if index_name == '#primary':
                    if (item.keyspace_id == self.collection_name or item.keyspace_id == self._bucket_name) \
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
                    item.keyspace_id == self.collection_name or item.keyspace_id == self._bucket_name):
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
                if item.keyspace_id == self.collection_name or item.keyspace_id == self._bucket_name:
                    return_list[item.name] = item.state
            return return_list
        except Exception as err:
            raise IndexNotReady(f"index_list: bucket {self._bucket.name} error: {err}")

    @retry(factor=0.5, allow_list=(IndexNotReady,))
    def delete_wait(self, index_name: str = None):
        if self.is_index(index_name=index_name):
            raise IndexNotReady(f"delete_wait: index still exists")

    def cb_map_upsert_attr(self, prefix: str, config: UpsertMapConfig, attr_obj: Any):
        primitive = (int, str, bool, list, dict)
        value = None
        for c in config.paths:
            logger.debug(f"cb_map_upsert: processing key {c.path} name {c.name}")
            try:
                subset = r_getattr(attr_obj, c.path)
                if isinstance(subset, list):
                    if not any(isinstance(item, primitive) for item in subset):
                        value = []
                        for item in subset:
                            value.append(item.as_dict)
                elif isinstance(subset, primitive) or not subset:
                    value = subset
                else:
                    value = subset.as_dict

                if c.collection:
                    self.create_collection(c.name)

                if c.exclude:
                    logger.debug(f"cb_map_upsert: excluding {','.join(c.exclude)}")
                    value = omit_path(value, c.exclude)

                if c.p_type == MapUpsertType.DOCUMENT:
                    doc_id = self.key_format(c.id, value, text=prefix)
                    logger.debug(f"cb_map_upsert: processing doc ID {doc_id}")
                    self._collection.upsert(doc_id, {c.name: value})
                elif c.p_type == MapUpsertType.LIST:
                    logger.debug(f"cb_map_upsert: processing list")
                    if not isinstance(value, list):
                        raise PathMapUpsertError(f"cb_map_upsert: path {c.path} type {type(value)} incompatible with list mode")
                    for doc in value:
                        doc_id = self.key_format(c.id, doc, text=prefix, id_key=c.id_key)
                        self._collection.upsert(doc_id, doc)

            except AttributeError:
                raise PathMapUpsertError(f"cb_map_upsert: key {c.path} not found")

            except Exception as err:
                raise PathMapUpsertError(f"cb_map_upsert: error {err}")

    @retry()
    def _cb_upsert(self, cluster, c_name, meta_id, doc_data, timeout=5):
        upsert_options = UpsertOptions(timeout=timedelta(seconds=timeout))
        try:
            logger.debug(f"upsert -> {c_name}: {meta_id}")
            bucket = cluster.bucket(self._bucket_name)
            collection = bucket.scope(self._scope_name).collection(c_name)
            result = collection.upsert(meta_id, doc_data, upsert_options)
            return result.cas
        except Exception as error:
            raise CollectionUpsertError(f"upsert error: {error}")

    @retry()
    def _create_collection(self, cluster, c_name):
        bucket = cluster.bucket(self._bucket_name)
        cm = bucket.collections()
        try:
            collection_spec = CollectionSpec(c_name, scope_name=self._scope_name)
            cm.create_collection(collection_spec, CreateCollectionOptions(timeout=timedelta(seconds=10)))
            self._verify_collection(cm, c_name)
        except CollectionAlreadyExistsException:
            pass
        except Exception as error:
            raise CollectionCreateException(f"collection create error: {error}")

    @retry()
    def _verify_collection(self, cm, c_name):
        sm = next((s for s in cm.get_all_scopes() if s.name == self._scope_name), None)
        valid = next((i for i in sm.collections if i.name == c_name), None)
        if not valid:
            raise CollectionCreateException(f"collection {c_name} was not created")

    def cb_map_upsert(self,
                      prefix: str,
                      config: UpsertMapConfig,
                      json_file: str = None,
                      xml_file: str = None,
                      json_data: str = None,
                      xml_data: str = None,
                      timeout=5):
        tasks = set()
        executor = concurrent.futures.ThreadPoolExecutor()
        cluster = Cluster.connect(self.cb_connect_string, self.cluster_options)

        if json_file:
            with open(json_file, mode="r") as json_xml:
                data = json.load(json_xml)
        elif xml_file:
            with open(xml_file, mode="rb") as input_xml:
                contents = input_xml.read()
                data = xmltodict.parse(contents)
        elif json_data:
            data = json.loads(json_data)
        elif xml_data:
            data = xmltodict.parse(xml_data)
        else:
            raise PathMapUpsertError(f"cb_map_upsert: JSON or XML input data is required")

        for c in config.paths:
            if c.collection:
                logger.debug(f"cb_map_upsert: creating collection {c.name}")
                tasks.add(executor.submit(self._create_collection, cluster, c.name))

        while tasks:
            done, tasks = concurrent.futures.wait(tasks, return_when=concurrent.futures.FIRST_COMPLETED)
            for task in done:
                try:
                    task.result()
                except Exception as err:
                    raise PathMapUpsertError(f"cb_map_upsert: {err}")

        tasks.clear()
        for c in config.paths:
            logger.debug(f"cb_map_upsert: processing key {c.path} name {c.name}")

            subset = copy_path(c.path, data)

            if not subset or len(subset) == 0:
                if c.optional:
                    continue
                else:
                    raise PathMapUpsertError(f"path {c.path} not found in source data")

            if c.collection:
                collection_name = c.name
            else:
                collection_name = self._collection.name

            if c.exclude:
                logger.debug(f"cb_map_upsert: excluding {','.join(c.exclude)}")
                subset = omit_path(subset, c.exclude)

            if c.p_type == MapUpsertType.DOCUMENT:
                doc_id = self.key_format(c.id, subset, text=prefix)
                logger.debug(f"cb_map_upsert: processing doc ID {doc_id}")
                doc = {c.name: subset}
                tasks.add(executor.submit(self._cb_upsert, cluster, collection_name, doc_id, doc, timeout))
            elif c.p_type == MapUpsertType.LIST:
                logger.debug(f"cb_map_upsert: processing list")
                if not isinstance(subset, list):
                    raise PathMapUpsertError(f"cb_map_upsert: path {c.path} type {type(subset)} incompatible with list mode")
                for doc in subset:
                    doc_id = self.key_format(c.id, doc, text=prefix, id_key=c.id_key)
                    tasks.add(executor.submit(self._cb_upsert, cluster, collection_name, doc_id, doc, timeout))

        while tasks:
            done, tasks = concurrent.futures.wait(tasks, return_when=concurrent.futures.FIRST_COMPLETED)
            for task in done:
                try:
                    task.result()
                except Exception as err:
                    raise PathMapUpsertError(f"cb_map_upsert: {err}")
