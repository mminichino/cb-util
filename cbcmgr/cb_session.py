##
##

from .exceptions import (DNSLookupTimeout, NodeUnreachable, NodeConnectionTimeout, NodeConnectionError,
                         NodeConnectionFailed, ClusterKVServiceError, ClusterHealthCheckError, ClusterQueryServiceError,
                         ClusterViewServiceError)
from .retry import retry
from .httpsessionmgr import APISession
import logging
import socket
import dns.resolver
import sys
from datetime import timedelta
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterTimeoutOptions, QueryOptions, ClusterOptions, LockMode
from couchbase.cluster import Cluster
from couchbase.diagnostics import ServiceType, PingState


class CBSession(object):

    def __init__(self, hostname: str, username: str, password: str, ssl=False, external=False):
        self.cluster_node_count = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self._cluster = None
        self._bucket = None
        self._scope = None
        self._collection = None
        self._scope_name = "_default"
        self._collection_name = "_default"
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
        self.timeouts = ClusterTimeoutOptions(query_timeout=timedelta(seconds=60),
                                              kv_timeout=timedelta(seconds=4),
                                              bootstrap_timeout=timedelta(seconds=4),
                                              resolve_timeout=timedelta(seconds=4),
                                              connect_timeout=timedelta(seconds=4),
                                              management_timeout=timedelta(seconds=4))

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
        self.logger.debug(f"Connect string: {connect_string}")
        return connect_string

    @retry(retry_count=5)
    def is_reachable(self):
        resolver = dns.resolver.Resolver()
        resolver.timeout = 5
        resolver.lifetime = 10

        self.logger.debug(f"checking if rally node is reachable: {self.rally_host_name}")
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
            self.logger.debug("process_cluster_data: no cluster info")
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
                    self.logger.debug(f"external address {rally_ip} detected")
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
        self.logger.debug(f"cluster {self.cb_connect_string} ping")
        ping_result = cluster.ping()
        endpoint: ServiceType
        for endpoint, reports in ping_result.endpoints.items():
            for report in reports:
                remote = report.remote.split(":")[0]
                nodes.append(remote)
                if not report.state == PingState.OK:
                    raise ClusterHealthCheckError(f"service {endpoint.value} not ok")

        node_set = set(nodes)
        self.logger.debug("ping complete")
        return list(node_set)

    @retry(factor=0.5)
    def wait_for_query_ready(self):
        query_str = r"SELECT 1 FROM system:dual;"
        cluster = Cluster(self.cb_connect_string, ClusterOptions(self.auth,
                                                                 timeout_options=self.timeouts,
                                                                 lockmode=LockMode.WAIT))
        result = cluster.query(query_str, QueryOptions(metrics=False, adhoc=True))
        value = result.rows()[0].get('$1')
        if value == 1:
            return True
        else:
            return False

    @retry(factor=0.5)
    def wait_for_index_ready(self):
        query_str = r"SELECT * FROM system:indexes;"
        cluster = Cluster(self.cb_connect_string, ClusterOptions(self.auth,
                                                                 timeout_options=self.timeouts,
                                                                 lockmode=LockMode.WAIT))
        result = cluster.query(query_str, QueryOptions(metrics=False, adhoc=True))
        value = result.rows()
        if len(value) >= 0:
            return True
        else:
            return False

    @retry()
    def cluster_health_check(self, output=False, restrict=True, noraise=False, extended=False):
        try:
            cluster = Cluster(self.cb_connect_string, ClusterOptions(self.auth,
                                                                     timeout_options=self.timeouts,
                                                                     lockmode=LockMode.WAIT))
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
                    if noraise:
                        print(f"{endpoint.value} service not ok: {report.state}")
                        sys.exit(2)
                    else:
                        if endpoint == ServiceType.KeyValue:
                            raise ClusterKVServiceError("{} KV service not ok".format(self.cb_connect_string))
                        elif endpoint == ServiceType.Query:
                            raise ClusterQueryServiceError("{} query service not ok".format(self.cb_connect_string))
                        elif endpoint == ServiceType.View:
                            raise ClusterViewServiceError("{} view service not ok".format(self.cb_connect_string))
                        else:
                            raise ClusterHealthCheckError(
                                "{} service {} not ok".format(self.cb_connect_string, endpoint.value))

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
                if noraise:
                    print(f"query service not ready: {err}")
                    sys.exit(3)
                else:
                    raise ClusterQueryServiceError(f"query service test error: {err}")

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
