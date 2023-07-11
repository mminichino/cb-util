##
##

from .exceptions import (DNSLookupTimeout, NodeUnreachable, NodeConnectionTimeout, NodeConnectionError, NodeConnectionFailed, ClusterHealthCheckError, KeyFormatError)
from .retry import retry
from .httpsessionmgr import APISession
from .config import KeyStyle
import logging
import socket
import dns.resolver
import uuid
import hashlib
from typing import Union
from enum import Enum
from datetime import timedelta
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterTimeoutOptions, LockMode, ClusterOptions, TLSVerifyMode
from couchbase.cluster import Cluster
from acouchbase.cluster import AsyncCluster
from couchbase.bucket import Bucket
from acouchbase.bucket import AsyncBucket
from couchbase.scope import Scope
from acouchbase.scope import AsyncScope
from couchbase.collection import Collection
from acouchbase.collection import AsyncCollection
from couchbase.diagnostics import ServiceType, PingState
from couchbase.management.buckets import CreateBucketSettings, BucketType, StorageBackend
from couchbase.management.collections import CollectionSpec
from couchbase.management.options import CreateQueryIndexOptions, CreatePrimaryQueryIndexOptions
from couchbase.exceptions import (BucketNotFoundException, ScopeNotFoundException, CollectionNotFoundException, BucketAlreadyExistsException, ScopeAlreadyExistsException,
                                  CollectionAlreadyExistsException, QueryIndexAlreadyExistsException)

logger = logging.getLogger('cbutil.session')
logger.addHandler(logging.NullHandler())


class BucketMode(Enum):
    DEFAULT = 0
    CACHE = 1
    CAPACITY = 2


class CBSession(object):
    HOSTNAME: str
    USERNAME: str
    PASSWORD: str
    SSL: bool
    EXTERNAL: bool

    def __init__(self, hostname: str, username: str, password: str, ssl=False, external=False, kv_timeout: int = 5, query_timeout: int = 60):
        self.cluster_node_count = None
        self._cluster = None
        self._bucket = None
        self._scope = None
        self._collection = None
        self._bucket_name = None
        self._scope_name = "_default"
        self._collection_name = "_default"
        self.kv_timeout = kv_timeout
        self.query_timeout = query_timeout
        self.hostname = hostname
        self.username = username
        self.password = password
        self.ssl = ssl
        self.rally_host_name = hostname
        self.rally_cluster_node = self.rally_host_name
        self.rally_dns_domain = False
        self.use_external_network = external
        self.external_network_present = False
        self.node_list = []
        self.external_list = []
        self.srv_host_list = []
        self.all_hosts = []
        self.node_cycle = None
        self.cluster_info = None
        self.sw_version = None
        self.memory_quota = None
        self.cluster_services = []
        self.auth = PasswordAuthenticator(self.username, self.password)
        self.timeouts = ClusterTimeoutOptions(query_timeout=timedelta(seconds=query_timeout),
                                              kv_timeout=timedelta(seconds=kv_timeout),
                                              bootstrap_timeout=timedelta(seconds=kv_timeout),
                                              resolve_timeout=timedelta(seconds=kv_timeout),
                                              connect_timeout=timedelta(seconds=kv_timeout),
                                              management_timeout=timedelta(seconds=kv_timeout))

        if self.ssl:
            self.prefix = "https://"
            self.cb_prefix = "couchbases://"
            self.srv_prefix = "_couchbases._tcp."
            self.admin_port = "18091"
            self.node_port = "19102"
        else:
            self.prefix = "http://"
            self.cb_prefix = "couchbase://"
            self.srv_prefix = "_couchbase._tcp."
            self.admin_port = "8091"
            self.node_port = "9102"

        self.cluster_options = ClusterOptions(self.auth,
                                              timeout_options=self.timeouts,
                                              tls_verify=TLSVerifyMode.NO_VERIFY,
                                              lockmode=LockMode.WAIT)
        if self.use_external_network:
            self.cluster_options.update(network="external")
        else:
            self.cluster_options.update(network="default")

        self.is_reachable()
        self.check_cluster()

        CBSession.HOSTNAME = self.hostname
        CBSession.USERNAME = self.username
        CBSession.PASSWORD = self.password
        CBSession.SSL = self.ssl
        CBSession.EXTERNAL = self.use_external_network

    def get_hostname(self) -> str:
        return self.hostname

    def get_username(self) -> str:
        return self.username

    def get_password(self) -> str:
        return self.password

    def get_ssl(self) -> bool:
        return self.ssl

    def get_external(self) -> bool:
        return self.use_external_network

    @retry()
    def session(self) -> Cluster:
        return Cluster.connect(self.cb_connect_string, self.cluster_options)

    @retry()
    async def session_a(self) -> AsyncCluster:
        cluster = await AsyncCluster.connect(self.cb_connect_string, self.cluster_options)
        await cluster.on_connect()
        return cluster

    @retry(always_raise_list=(BucketNotFoundException,))
    def get_bucket(self, cluster: Cluster, name: str) -> Bucket:
        if name is None:
            raise TypeError("name can not be None")
        logger.debug(f"bucket: connect {name}")
        return cluster.bucket(name)

    @retry()
    def _create_bucket(self, cluster: Cluster, name: str, quota: int = 256, replicas: int = 0, mode: BucketMode = BucketMode.DEFAULT):
        if name is None:
            raise TypeError("name can not be None")

        logger.debug(f"creating bucket {name}")

        if mode == BucketMode.DEFAULT:
            b_type = BucketType.COUCHBASE
            b_stor = StorageBackend.COUCHSTORE
        elif mode == BucketMode.CACHE:
            b_type = BucketType.EPHEMERAL
            b_stor = StorageBackend.COUCHSTORE
        else:
            b_type = BucketType.COUCHBASE
            b_stor = StorageBackend.MAGMA

        try:
            bm = cluster.buckets()
            bm.create_bucket(CreateBucketSettings(name=name,
                                                  bucket_type=b_type,
                                                  storage_backend=b_stor,
                                                  num_replicas=replicas,
                                                  ram_quota_mb=quota))
        except BucketAlreadyExistsException:
            pass

    @retry(always_raise_list=(BucketNotFoundException,))
    async def get_bucket_a(self, cluster: AsyncCluster, name: str) -> AsyncBucket:
        if name is None:
            raise TypeError("name can not be None")
        logger.debug(f"bucket: connect {name}")
        bucket = cluster.bucket(name)
        await bucket.on_connect()
        return bucket

    @retry(always_raise_list=(ScopeNotFoundException,))
    def get_scope(self, bucket: Bucket, name: str = "_default") -> Scope:
        if name is None:
            raise TypeError("name can not be None")
        logger.debug(f"scope: connect {name}")
        if not self.is_scope(bucket, name):
            raise ScopeNotFoundException(f"scope {name} does not exist")
        return bucket.scope(name)

    @retry()
    def _create_scope(self, bucket: Bucket, name: str):
        if name is None:
            raise TypeError("name can not be None")

        try:
            if name != "_default":
                cm = bucket.collections()
                cm.create_scope(name)
        except ScopeAlreadyExistsException:
            pass

    @retry(always_raise_list=(ScopeNotFoundException,))
    async def get_scope_a(self, bucket: AsyncBucket, name: str = "_default") -> AsyncScope:
        if name is None:
            raise TypeError("name can not be None")
        logger.debug(f"scope: connect {name}")
        scope = bucket.scope(name)
        return scope

    @retry(always_raise_list=(CollectionNotFoundException,))
    def get_collection(self, bucket: Bucket, scope: Scope, name: str = "_default") -> Collection:
        if name is None:
            raise TypeError("name can not be None")
        logger.debug(f"collection: connect {name}")
        if not self.is_collection(bucket, scope.name, name):
            raise CollectionNotFoundException(f"collection {name} does not exist")
        return scope.collection(name)

    def _create_collection(self, bucket: Bucket, scope: Scope, name: str):
        if name is None:
            raise TypeError("name can not be None")

        try:
            if name != "_default":
                collection_spec = CollectionSpec(name, scope_name=scope.name)
                cm = bucket.collections()
                cm.create_collection(collection_spec)
        except CollectionAlreadyExistsException:
            pass

    @retry()
    def _create_indexes(self, cluster: Cluster, bucket: Bucket, scope: Scope, collection: Collection, fields: list[str], replica: int = 0):
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
        except QueryIndexAlreadyExistsException:
            logger.debug(f"index already exists")
            pass

    @retry()
    def _create_primary_index(self, cluster: Cluster, bucket: Bucket, scope: Scope, collection: Collection, replica: int = 0):
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
        except QueryIndexAlreadyExistsException:
            pass

    @retry(always_raise_list=(CollectionNotFoundException,))
    async def get_collection_a(self, scope: AsyncScope, name: str = "_default") -> AsyncCollection:
        if name is None:
            raise TypeError("name can not be None")
        logger.debug(f"collection: connect {name}")
        collection = scope.collection(name)
        return collection

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

    def construct_key(self, key):
        if type(key) == int or str(key).isdigit():
            if self._collection.name != "_default":
                return self._collection.name + ':' + str(key)
            else:
                return self._bucket.name + ':' + str(key)
        else:
            return key

    @property
    def keyspace(self):
        if self._scope_name != "_default" or self._collection_name != "_default":
            return self._bucket.name + '.' + self._scope_name + '.' + self._collection_name
        else:
            return self._bucket.name

    @property
    def collection_name(self):
        if self._collection_name == "_default":
            return self._bucket.name
        else:
            return self._collection_name

    @property
    def cb_connect_string(self):
        connect_string = self.cb_prefix + self.rally_host_name
        logger.debug(f"Connect string: {connect_string}")
        return connect_string

    @retry(retry_count=5)
    def is_reachable(self):
        resolver = dns.resolver.Resolver()
        resolver.timeout = 5
        resolver.lifetime = 10

        logger.debug(f"checking if rally node is reachable: {self.rally_host_name}")
        try:
            answer = resolver.resolve(self.srv_prefix + self.rally_host_name, "SRV")
            for srv in answer:
                record = {'hostname': str(srv.target).rstrip('.')}
                host_answer = resolver.resolve(record['hostname'], 'A')
                record['address'] = host_answer[0].address
                self.srv_host_list.append(record)
            self.rally_cluster_node = self.srv_host_list[0]['hostname']
            self.rally_dns_domain = True
        except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer):
            pass
        except dns.exception.Timeout:
            raise DNSLookupTimeout(f"{self.srv_prefix + self.rally_host_name} lookup timeout")
        except Exception:
            raise

        if self.rally_dns_domain:
            self.rally_cluster_node = self.rally_host_name = self.srv_host_list[0]['hostname']
            logger.debug(f"Rewriting rally node as {self.rally_cluster_node}")

        try:
            self.check_node_connectivity(self.rally_cluster_node, self.admin_port)
        except (NodeConnectionTimeout, NodeConnectionError, NodeConnectionFailed) as err:
            raise NodeUnreachable(f"can not connect to node {self.rally_cluster_node}: {err}")

        return True

    def check_cluster(self):
        s = APISession(self.username, self.password)
        s.set_host(self.rally_host_name, self.ssl, self.admin_port)
        self.cluster_info = s.api_get('/pools/default').json()
        self.process_cluster_data()

    def process_cluster_data(self):
        rally_ip = socket.gethostbyname(self.rally_host_name)

        if not self.cluster_info:
            logger.debug("process_cluster_data: no cluster info")
            return

        self.cluster_node_count = range(len(self.cluster_info['nodes']))
        self.sw_version = self.cluster_info['nodes'][0]['version']

        for node in self.cluster_info['nodes']:
            node_name = node.get("configuredHostname").split(':')[0]
            alternate_address = node.get("alternateAddresses", {}).get("external", {}).get("hostname")
            self.node_list.append(node_name)
            if alternate_address:
                self.external_list.append(alternate_address)
                external_ip = socket.gethostbyname(alternate_address)
                if rally_ip == external_ip:
                    logger.debug(f"external address {rally_ip} detected")
                    self.use_external_network = True

    @retry(retry_count=5)
    def check_node_connectivity(self, hostname, port):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex((hostname, int(port)))
            sock.close()
        except socket.timeout:
            raise NodeConnectionTimeout(f"timeout connecting to {hostname}:{port}")
        except socket.error as err:
            raise NodeConnectionError(f"error connecting to {hostname}:{port}: {err}")

        if result == 0:
            return True
        else:
            raise NodeConnectionFailed(f"node {hostname}:{port} unreachable")

    @retry(factor=0.5)
    def wait_until_ready(self):
        nodes = []
        cluster = Cluster(self.cb_connect_string, ClusterOptions(self.auth,
                                                                 timeout_options=self.timeouts,
                                                                 lockmode=LockMode.WAIT))
        logger.debug(f"cluster {self.cb_connect_string} ping")
        ping_result = cluster.ping()
        endpoint: ServiceType
        for endpoint, reports in ping_result.endpoints.items():
            for report in reports:
                remote = report.remote.split(":")[0]
                nodes.append(remote)
                if not report.state == PingState.OK:
                    raise ClusterHealthCheckError(f"service {endpoint.value} not ok")

        node_set = set(nodes)
        logger.debug("ping complete")
        return list(node_set)

    def print_host_map(self):
        if self.rally_dns_domain:
            print("Name %s is a domain with SRV records:" % self.rally_host_name)
            for record in self.srv_host_list:
                print(" => %s (%s)" % (record['hostname'], record['address']))

        print("Cluster Host List:")
        for i, record in enumerate(self.cluster_info['nodes']):
            if 'alternateAddresses' in record:
                ext_host_name = record['alternateAddresses']['external']['hostname']
                ext_port_list = record['alternateAddresses']['external']['ports']
            else:
                ext_host_name = None
                ext_port_list = None
            host_name = record['configuredHostname']
            version = record['version']
            ostype = record['os']
            services = ','.join(record['services'])
            print(" [%02d] %s" % (i + 1, host_name), end=' ')
            if ext_host_name:
                print("[external]> %s" % ext_host_name, end=' ')
            if ext_port_list:
                for key in ext_port_list:
                    print("%s:%s" % (key, ext_port_list[key]), end=' ')
            print("[Services] %s [version] %s [platform] %s" % (services, version, ostype))

    def key_format(self,
                   style: KeyStyle,
                   document: dict,
                   doc_num: int = 1,
                   id_key: str = "record_id",
                   separator: str = "::",
                   text: Union[str, None] = None,
                   path: Union[str, None] = None,
                   field: Union[str, None] = None):
        if style.value == 0:
            return f"{self.keyspace}{separator}{doc_num}"
        elif style.value == 1:
            if not document.get('type'):
                raise KeyFormatError(f"Key style type requested: document does not have type field")
            if document.get(id_key):
                number = document.get(id_key)
            else:
                number = doc_num
            return f"{document['type']}{separator}{number}"
        elif style.value == 2:
            return uuid.uuid4()
        elif style.value == 3:
            if not field:
                raise KeyFormatError(f"Key field name style requested: field parameter is null")
            return f"{field}{separator}{doc_num}"
        elif style.value == 4:
            return f"{self.keyspace}{separator}{doc_num}"
        elif style.value == 5:
            if not field:
                raise KeyFormatError(f"Key compound name style requested: field parameter is null")
            return f"{self.keyspace}{separator}{field}{separator}{doc_num}"
        elif style.value == 6:
            if not path or not text:
                raise KeyFormatError(f"Key path style parameters not provided")
            return f"{text}{separator}{path}"
        elif style.value == 7:
            if not text:
                raise KeyFormatError(f"Key text style parameter not provided")
            return f"{text}"
        elif style.value == 8:
            if not text:
                raise KeyFormatError(f"Key text style parameter not provided")
            number = document.get(id_key, 1)
            return f"{text}{separator}{id_key}{separator}{number}"
        else:
            raise KeyFormatError(f"Unknown key style {style.name}")
