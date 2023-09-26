#!/usr/bin/env python3

import warnings
from cbcmgr.cb_capella import Capella, CapellaCluster, AllowedCIDR, Credentials, Bucket


warnings.filterwarnings("ignore")


def test_capella_1():
    result = Capella().list_projects()
    project_id = result.get('pytest-project')

    assert project_id is not None

    cluster = CapellaCluster().create("pytest-cluster", "Pytest created cluster", "aws", "us-east-2")
    cluster.add_service_group("aws", "4x16")

    cluster_id = Capella(project_id=project_id).create_cluster(cluster)

    assert cluster_id is not None

    result = Capella(project_id=project_id).wait_cluster("pytest-cluster")

    assert result is True

    cidr = AllowedCIDR().create()
    cidr_id = Capella(project_id=project_id).allow_cidr(cluster_id, cidr)

    assert cidr_id is not None

    credentials = Credentials().create("sysdba", "Passw0rd!")
    account_id = Capella(project_id=project_id).add_db_user(cluster_id, credentials)

    assert account_id is not None

    bucket = Bucket().create("employees", 128)
    bucket_id = Capella(project_id=project_id).add_bucket(cluster_id, bucket)

    assert bucket_id is not None


test_capella_1()
