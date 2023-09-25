#!/usr/bin/env python3

import warnings
from cbcmgr.cb_capella import Capella, CapellaCluster, CloudProvider, ServiceGroup, Availability, Support, NodeConfig, ComputeConfig, StorageConfig


warnings.filterwarnings("ignore")


def test_capella_1():
    result = Capella().list_projects()
    project_id = result.get('pytest-project')

    assert project_id is not None

    cluster = CapellaCluster(
        "pytest-cluster",
        "Pytest created cluster",
        CloudProvider(
            "aws",
            "us-east-2"
        ),
        [
            ServiceGroup(
                NodeConfig(
                    ComputeConfig(4, 16),
                    StorageConfig(256, "gp3", 6000)
                ),
                3,
                ["data", "index", "query"]
            )
        ],
        Availability(),
        Support()
    )

    cluster_id = Capella(project_id=project_id).create_cluster(cluster)

    assert cluster_id is not None

    result = Capella(project_id=project_id).wait_cluster("pytest-cluster")

    assert result is True


test_capella_1()
