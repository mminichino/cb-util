##
##

from .exceptions import (IndexInternalError, CollectionGetError, CollectionCountError, BucketCreateException)
from .retry import retry
from .cb_session import CBSession, BucketMode
from .cb_bucket import Bucket as CouchbaseBucket
from .cb_index import CBQueryIndex
from .cb_capella import Capella, Credentials
import logging
import hashlib
import attr
from datetime import timedelta
from typing import Union, Dict, Any, List
from couchbase.cluster import Cluster
from couchbase.bucket import Bucket
from couchbase.scope import Scope
from couchbase.collection import Collection
from couchbase.options import QueryOptions
from couchbase.management.users import Role, User, Group
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
    def create_bucket(self, cluster: Cluster, name: str, quota: int = 256, replicas: int = 0, max_ttl: int = 0, flush: bool = False, mode: BucketMode = BucketMode.DEFAULT):
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

        bucket_opts = CouchbaseBucket(**dict(
            name=name,
            ram_quota_mb=quota,
            bucket_type=b_type,
            storage_backend=b_stor,
            num_replicas=replicas,
            max_ttl=max_ttl,
            flush_enabled=flush
        ))

        if self.capella_project and self.capella_db:
            project = Capella().get_project(self.capella_project)
            if not project:
                raise BucketCreateException(f"Can not lookup Capella project {self.capella_project}")
            project_id = project.get('id')
            cluster = Capella(project_id=project_id).get_cluster(self.capella_db)
            if not cluster:
                raise BucketCreateException(f"Can not find Capella database {self.capella_db}")
            cluster_id = cluster.get('id')
            logger.debug(f"Creating Capella bucket {bucket_opts.name} in project {project_id} database {cluster_id}")
            Capella(project_id=project_id).add_bucket(cluster_id, bucket_opts)
        else:
            try:
                bm = cluster.buckets()
                # noinspection PyTypeChecker
                bm.create_bucket(CreateBucketSettings(**attr.asdict(bucket_opts)))
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

    @property
    def user_list(self):
        if self._cluster is None:
            raise ValueError("cluster not connected")
        results = []
        um = self._cluster.users()
        for user in [u.user.as_dict() for u in um.get_all_users()]:
            output = {
                "username": user.get('username'),
                "name": user.get('name'),
                "password": user.get('password')
            }
            if user.get('roles'):
                output.update({'roles': [r.as_dict() for r in user.get('roles')]})
            if user.get('groups'):
                output.update({'groups': [g for g in user.get('groups')]})
            results.append(output)
        return results

    @property
    def group_list(self):
        if self._cluster is None:
            raise ValueError("cluster not connected")
        results = []
        um = self._cluster.users()
        for group in [g.as_dict() for g in um.get_all_groups()]:
            results.append(group)
        return results

    def create_user(self, username: str, name: str = None, password: str = None, roles: List[Role] = None, groups: List[str] = None):
        if self._cluster is None:
            raise ValueError("cluster not connected")
        if not roles or len(roles) == 0:
            roles = [
                Role(name="data_reader", bucket="*"),
                Role(name="query_select", bucket="*"),
                Role(name="data_writer", bucket="*"),
                Role(name="query_insert", bucket="*"),
                Role(name="query_delete", bucket="*"),
                Role(name="query_manage_index", bucket="*"),
            ]
        if not password:
            password = Capella().generate_password()
            logger.info(f"Password: {password}")

        if self.capella_project and self.capella_db:
            project = Capella().get_project(self.capella_project)
            if not project:
                raise BucketCreateException(f"Can not lookup Capella project {self.capella_project}")
            project_id = project.get('id')
            cluster = Capella(project_id=project_id).get_cluster(self.capella_db)
            if not cluster:
                raise BucketCreateException(f"Can not find Capella database {self.capella_db}")
            cluster_id = cluster.get('id')
            credentials = Credentials().from_cbs(username, password, roles)
            Capella(project_id=project_id).add_db_user(cluster_id, credentials)
        else:
            um = self._cluster.users()
            # noinspection PyTypeChecker
            user = User(username=username, display_name=name, password=password, roles=roles)
            if groups and len(groups) > 0:
                user.groups = set(groups)
            um.upsert_user(user)

    def create_group(self, name: str = None, description: str = None, roles: List[Role] = None):
        if self._cluster is None:
            raise ValueError("cluster not connected")
        if not roles or len(roles) == 0:
            roles = [
                Role(name="data_reader", bucket="*"),
                Role(name="query_select", bucket="*"),
                Role(name="data_writer", bucket="*"),
                Role(name="query_insert", bucket="*"),
                Role(name="query_delete", bucket="*"),
                Role(name="query_manage_index", bucket="*"),
            ]
        if self.capella_project and self.capella_db:
            logger.warning("Skipping group creation on Capella")
        else:
            for role in roles:
                print(role.as_dict())
            um = self._cluster.users()
            # noinspection PyTypeChecker
            group = Group(name=name, description=description, roles=roles)
            um.upsert_group(group)

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
    def index_create(self, index: CBQueryIndex, timeout: int = 480):
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

    @property
    def index_list(self):
        contents = []
        all_list = []
        query_str = r"SELECT * FROM system:indexes ;"
        results = self._cluster.query(query_str, QueryOptions(metrics=False, adhoc=True))

        for item in results:
            contents.append(item)

        for row in contents:
            for key, value in row.items():
                entry = CBQueryIndex.from_dict(value)
                all_list.append(entry)

        return all_list

    @property
    def bucket_list(self):
        if self._cluster is None:
            raise ValueError("cluster not connected")
        bm = self._cluster.buckets()
        return bm.get_all_buckets()

    def scope_list(self, bucket: str):
        bucket = self._cluster.bucket(bucket)
        cm = bucket.collections()
        return cm.get_all_scopes()

    def collection_list(self, bucket: str, scope: str):
        bucket = self._cluster.bucket(bucket)
        cm = bucket.collections()
        scope_obj = next((s for s in cm.get_all_scopes() if s.name == scope), None)
        if not scope_obj:
            raise ValueError(f"scope {scope} not found")
        return scope_obj.collections
