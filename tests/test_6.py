#!/usr/bin/env python3

import warnings
import time
from cbcmgr.cb_capella import Capella, CapellaCluster, AllowedCIDR, Credentials
from cbcmgr.cb_bucket import Bucket

warnings.filterwarnings("ignore")


class TestCapella(object):

    def test_1(self):
        project = Capella().get_project('pytest-project')
        project_id = project.get('id')

        assert project_id is not None

        cluster = CapellaCluster().create("pytest-cluster", "Pytest created cluster", "aws", "us-east-2")
        cluster.add_service_group("aws", "4x16")

        print("Creating cluster")
        cluster_id = Capella(project_id=project_id).create_cluster(cluster)

        assert cluster_id is not None

        print("Waiting for cluster creation to complete")
        result = Capella(project_id=project_id).wait_for_cluster("pytest-cluster")

        assert result is True

        cidr = AllowedCIDR().create()

        print("Creating allowed CIDR")
        cidr_id = Capella(project_id=project_id).allow_cidr(cluster_id, cidr)

        assert cidr_id is not None

        credentials = Credentials().create("sysdba", "Passw0rd!")

        print("Creating database credentials")
        account_id = Capella(project_id=project_id).add_db_user(cluster_id, credentials)

        assert account_id is not None

        bucket = Bucket(**dict(
            name="employees",
            ram_quota_mb=128
        ))

        print("Creating bucket")
        bucket_id = Capella(project_id=project_id).add_bucket(cluster_id, bucket)

        assert bucket_id is not None
        time.sleep(1)

        print("Deleting bucket")
        Capella(project_id=project_id).delete_bucket("pytest-cluster", "employees")

        print("Deleting cluster")
        Capella(project_id=project_id).delete_cluster("pytest-cluster")
        print("Waiting for cluster deletion to complete")
        result = Capella(project_id=project_id).wait_for_cluster_delete("pytest-cluster")

        assert result is True
