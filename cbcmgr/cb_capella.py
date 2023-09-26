##
##

import logging
import attr
import os
import re
import time
from typing import Optional, List, Union
from enum import Enum
import attrs
import ipaddress
from itertools import cycle
from ipaddress import IPv4Network
from cbcmgr.httpsessionmgr import APISession, AuthType
from cbcmgr.exceptions import CapellaError, APIError

logger = logging.getLogger('cbcmgr.capella')
logger.addHandler(logging.NullHandler())


aws_storage_matrix = {
    99: 3000,
    199: 5000,
    299: 6000,
    399: 8000,
    499: 9000,
    599: 10000,
    699: 12000,
    799: 13000,
    899: 14000,
    999: 16000,
    16384: 16000
}


azure_storage_matrix = {
    64: "P6",
    128: "P10",
    256: "P15",
    512: "P20",
    1024: "P30",
    2048: "P40",
    4096: "P50",
    8192: "P60"
}


class NodeAvailability(str, Enum):
    single = 'single'
    multi = 'multi'


class SupportPlan(str, Enum):
    basic = 'basic'
    devpro = 'developer pro'
    enterprise = 'enterprise'


class SupportTZ(str, Enum):
    eastern_us = 'ET'
    emea = 'GMT'
    asia = 'IST'
    western_us = 'PT'


class BucketType(str, Enum):
    couchbase = 'couchbase'
    ephemeral = 'ephemeral'


class BucketBackend(str, Enum):
    couchstore = 'couchstore'
    magma = 'magma'


class BucketResolution(str, Enum):
    seqno = 'seqno'
    lww = 'lww'


class BucketDurability(str, Enum):
    none = 'none'
    majority = 'majority'
    majorityAndPersistActive = 'majorityAndPersistActive'
    persistToMajority = 'persistToMajority'


class BucketEviction(str, Enum):
    fullEviction = 'fullEviction'
    noEviction = 'noEviction'
    nruEviction = 'nruEviction'


@attr.s
class CloudProvider:
    type: Optional[str] = attr.ib(default=None)
    region: Optional[str] = attr.ib(default=None)
    cidr: Optional[str] = attr.ib(default='10.0.0.0/23')


@attr.s
class ComputeConfig:
    cpu: Optional[int] = attr.ib(default=None)
    ram: Optional[int] = attr.ib(default=None)


@attr.s
class StorageConfig:
    storage: Optional[int] = attr.ib(default=None)
    type: Optional[str] = attr.ib(default=None)
    iops: Optional[int] = attr.ib(default=None)


@attr.s
class NodeConfig:
    compute: Optional[ComputeConfig] = attr.ib(default=None)
    disk: Optional[StorageConfig] = attr.ib(default=None)


@attr.s
class ServiceGroup:
    node: Optional[NodeConfig] = attr.ib(default=None)
    numOfNodes: Optional[int] = attr.ib(default=None)
    services: Optional[List[str]] = attr.ib(default=None)


@attr.s
class Availability:
    type: Optional[NodeAvailability] = attr.ib(default=NodeAvailability.multi)


@attr.s
class Support:
    plan: Optional[SupportPlan] = attr.ib(default=SupportPlan.devpro)
    timezone: Optional[SupportTZ] = attr.ib(default=SupportTZ.western_us)


@attr.s
class CapellaCluster:
    name: Optional[str] = attr.ib(default=None)
    description: Optional[str] = attr.ib(default=None)
    cloudProvider: Optional[CloudProvider] = attr.ib(default=None)
    serviceGroups: Optional[List[ServiceGroup]] = attr.ib(default=[])
    availability: Optional[Availability] = attr.ib(default=None)
    support: Optional[Support] = attr.ib(default=None)

    @classmethod
    def create(cls, name, description, cloud, region, availability=NodeAvailability.multi, plan=SupportPlan.devpro, timezone=SupportTZ.western_us):
        return cls(
            name,
            description,
            CloudProvider(
                cloud,
                region
            ),
            [],
            Availability(availability),
            Support(plan, timezone)
        )

    def add_service_group(self, cloud, machine_type, storage=256, quantity=3, services=None):
        if not services:
            services = ["data", "index", "query"]
        cpu, memory = machine_type.split('x')
        if cloud == "aws":
            size = storage
            iops = next((aws_storage_matrix[s] for s in aws_storage_matrix if s >= storage), None)
            s_type = "gp3"
        elif cloud == "azure":
            size, s_type = next(((s, azure_storage_matrix[s]) for s in azure_storage_matrix if s >= storage), None)
            iops = None
        else:
            size = storage
            s_type = None
            iops = None
        self.serviceGroups.append(
            ServiceGroup(
                NodeConfig(
                    ComputeConfig(int(cpu), int(memory)),
                    StorageConfig(size, s_type, iops)
                ),
                quantity,
                services
            )
        )


@attr.s
class AllowedCIDR:
    cidr: Optional[str] = attr.ib(default='0.0.0.0/0')

    @classmethod
    def create(cls, cidr='0.0.0.0/0'):
        return cls(
            cidr
        )


@attr.s
class UserAccess:
    privileges: Optional[List[str]] = attr.ib(default=["read", "write"])


@attr.s
class Credentials:
    name: Optional[str] = attr.ib(default='sysdba')
    password: Optional[str] = attr.ib(default=None)
    access: Optional[List[UserAccess]] = attr.ib(default=None)

    @classmethod
    def create(cls, username, password):
        return cls(
            username,
            password,
            [
                UserAccess()
            ]
        )


@attr.s
class Bucket:
    name: Optional[str] = attr.ib(default=None)
    type: Optional[str] = attr.ib(default=None)
    storageBackend: Optional[str] = attr.ib(default=None)
    memoryAllocationInMb: Optional[str] = attr.ib(default=None)
    bucketConflictResolution: Optional[BucketResolution] = attr.ib(default=None)
    durabilityLevel: Optional[BucketDurability] = attr.ib(default=None)
    replicas: Optional[int] = attr.ib(default=None)
    flush: Optional[bool] = attr.ib(default=None)
    timeToLiveInSeconds: Optional[int] = attr.ib(default=None)
    evictionPolicy: Optional[BucketEviction] = attr.ib(default=None)

    @classmethod
    def create(cls, name, quota, replicas=1, ttl=0, bucket_type=BucketType.couchbase, backend=BucketBackend.couchstore):
        return cls(
            name,
            bucket_type,
            backend,
            quota,
            BucketResolution.seqno,
            BucketDurability.none,
            replicas,
            False,
            ttl,
            BucketEviction.fullEviction
        )


class NetworkDriver(object):

    def __init__(self):
        self.ip_space = []
        self.active_network: IPv4Network = ipaddress.ip_network("10.1.0.0/16")
        self.super_net: IPv4Network = ipaddress.ip_network("10.0.0.0/8")

    def set_active_network(self, cidr: str):
        self.active_network: IPv4Network = ipaddress.ip_network(cidr)

    def add_network(self, cidr: str) -> None:
        cidr_net = ipaddress.ip_network(cidr)
        self.ip_space.append(cidr_net)

    def get_next_subnet(self, prefix=24) -> str:
        for subnet in self.active_network.subnets(new_prefix=prefix):
            yield subnet.exploded

    def get_next_network(self) -> Union[str, None]:
        candidates = list(self.super_net.subnets(new_prefix=16))

        for network in self.ip_space:
            available = []
            for n, candidate in enumerate(candidates):
                try:
                    if network.prefixlen < 16:
                        list(network.address_exclude(candidate))
                    else:
                        list(candidate.address_exclude(network))
                except ValueError:
                    available.append(candidate)
            candidates = available

        if len(candidates) == 0:
            return None

        self.active_network = candidates[0]
        self.ip_space.append(self.active_network)
        return self.active_network.exploded


class Capella(APISession):

    def __init__(self, *args, organization_id=None, project_id=None, **kwargs):
        super().__init__(*args, auth_type=AuthType.capella, **kwargs)
        self._cluster_id = None
        self._cluster_name = None
        self.organization_id = organization_id
        self.project_id = project_id

        if 'CAPELLA_API_URL' in os.environ:
            self._api_host = os.environ['CAPELLA_API_URL']
        else:
            self._api_host = "cloudapi.cloud.couchbase.com"

        self.set_host(self._api_host, ssl=APISession.HTTPS)

        if not self.organization_id:
            orgs = self.list_organizations()
            try:
                self.organization_id = orgs[0]
            except IndexError:
                raise CapellaError("please provide an organization ID (unable to automatically determine default ID")

    def list_organizations(self):
        organizations = []
        results = self.api_get(f"/v4/organizations").json()
        for entry in results.get('data', []):
            organizations.append(entry.get('id'))
        return organizations

    def list_projects(self):
        results = self.api_get(f"/v4/organizations/{self.organization_id}/projects").json()

        projects = dict((i.get('name'), i.get('id')) for i in results)
        return projects

    def list_clusters(self):
        results = self.api_get(f"/v4/organizations/{self.organization_id}/projects/{self.project_id}/clusters").json()

        return results

    def get_cluster(self, name):
        results = self.api_get(f"/v4/organizations/{self.organization_id}/projects/{self.project_id}/clusters").json()

        return next((c for c in results if c.get('name') == name), None)

    def create_cluster(self, cluster: CapellaCluster):
        cidr_util = NetworkDriver()
        # noinspection PyTypeChecker
        parameters = attrs.asdict(cluster)
        cluster_cidr = cluster.cloudProvider.cidr
        cidr_util.add_network(cluster_cidr)
        cidr_util.get_next_network()
        subnet_list = list(cidr_util.get_next_subnet(prefix=23))
        subnet_cycle = cycle(subnet_list)

        response = self.get_cluster(cluster.name)
        if response:
            return response.get('id')

        while True:
            try:
                results = self.api_post(f"/v4/organizations/{self.organization_id}/projects/{self.project_id}/clusters", body=parameters).json()
                return results.get('id')
            except APIError as err:
                match = re.search(r"The provided CIDR of .* is not unique within this organization", err.body.get('message', ''))
                if match:
                    logger.debug(f"Provided CIDR {cluster.cloudProvider.cidr} is in use in the organization")
                    network_cidr = next(subnet_cycle)
                    cluster.cloudProvider.cidr = network_cidr
                    # noinspection PyTypeChecker
                    parameters = attrs.asdict(cluster)
                    logger.debug(f"Trying new CIDR {network_cidr}")
                else:
                    raise CapellaError(f"Can not create Capella database: {err} message: {err.body.get('message', '')}")

    def wait_cluster(self, name, retry_count=120):
        for retry_number in range(retry_count + 1):
            cluster = self.get_cluster(name)
            if cluster.get('currentState') == 'healthy':
                return True
            else:
                if retry_number == retry_count:
                    return False
                logger.info(f"Waiting for cluster {name} to deploy")
                time.sleep(5)

    def get_allowed_cidr(self, cluster_id: str, cidr: str):
        results = self.api_get(f"/v4/organizations/{self.organization_id}/projects/{self.project_id}/clusters/{cluster_id}/allowedcidrs").json()

        return next((c for c in results if c.get('cidr') == cidr), None)

    def allow_cidr(self, cluster_id: str, cidr: AllowedCIDR):
        response = self.get_allowed_cidr(cluster_id, cidr.cidr)
        if response:
            return response.get('id')

        # noinspection PyTypeChecker
        parameters = attrs.asdict(cidr)

        try:
            results = self.api_post(f"/v4/organizations/{self.organization_id}/projects/{self.project_id}/clusters/{cluster_id}/allowedcidrs", body=parameters).json()
            return results.get('id')
        except APIError as err:
            raise CapellaError(f"Can not add database allowed CIDR: {err} message: {err.body.get('message', '')}")

    def get_db_user(self, cluster_id: str, username: str):
        results = self.api_get(f"/v4/organizations/{self.organization_id}/projects/{self.project_id}/clusters/{cluster_id}/users").json()

        return next((c for c in results if c.get('name') == username), None)

    def add_db_user(self, cluster_id: str, credentials: Credentials):
        response = self.get_db_user(cluster_id, credentials.name)
        if response:
            return response.get('id')

        # noinspection PyTypeChecker
        parameters = attrs.asdict(credentials)

        try:
            results = self.api_post(f"/v4/organizations/{self.organization_id}/projects/{self.project_id}/clusters/{cluster_id}/users", body=parameters).json()
            return results.get('id')
        except APIError as err:
            raise CapellaError(f"Can not add database user: {err} message: {err.body.get('message', '')}")

    def get_bucket(self, cluster_id: str, bucket: str):
        results = self.api_get(f"/v4/organizations/{self.organization_id}/projects/{self.project_id}/clusters/{cluster_id}/buckets").json()

        return next((c for c in results if c.get('name') == bucket), None)

    def add_bucket(self, cluster_id: str, bucket: Bucket):
        response = self.get_bucket(cluster_id, bucket.name)
        if response:
            return response.get('id')

        # noinspection PyTypeChecker
        parameters = attrs.asdict(bucket)

        try:
            results = self.api_post(f"/v4/organizations/{self.organization_id}/projects/{self.project_id}/clusters/{cluster_id}/buckets", body=parameters).json()
            return results.get('id')
        except APIError as err:
            raise CapellaError(f"Can not add bucket: {err} message: {err.body.get('message', '')}")

    def delete_bucket(self, cluster_id: str, bucket_id: str):
        results = self.api_delete(f"/v4/organizations/{self.organization_id}/projects/{self.project_id}/clusters/{cluster_id}/buckets/{bucket_id}")
        return results
