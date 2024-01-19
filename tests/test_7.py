#!/usr/bin/env python3

import warnings
import logging
import time
import pytest
from cbcmgr.restmgr import RESTManager
from cbcmgr.cb_capella_config import CapellaConfigFile

warnings.filterwarnings("ignore")
logger = logging.getLogger()


@pytest.mark.serial
class TestRESTManager(object):
    api_host = None

    @classmethod
    def setup_class(cls):
        logging.basicConfig()
        logger.setLevel(logging.DEBUG)

    def test_1(self):
        profile = 'default'
        rest = RESTManager(profile=profile)
        cf = CapellaConfigFile(profile)
        org_id = rest.get_capella('/v4/organizations').item(0).id()

        start_time = time.perf_counter_ns()
        result = rest.get_capella(f"/v4/organizations/{org_id}/projects").list()
        end_time = time.perf_counter_ns()
        time_diff = end_time - start_time
        print(f"Items: {len(result)} in {time_diff / 1000000}: OK")

        start_time = time.perf_counter_ns()
        result = rest.get_capella(f"/v4/organizations/{org_id}/projects").by_name('pytest-project').unique().id()
        end_time = time.perf_counter_ns()
        time_diff = end_time - start_time
        print(f"Item: {result} in {time_diff / 1000000}: OK")

        start_time = time.perf_counter_ns()
        result = rest.get_capella_kv(f"/v4/organizations/{org_id}/users", "email", cf.account_email).item(0).record()
        end_time = time.perf_counter_ns()
        time_diff = end_time - start_time
        print(f"Item: {result.get('name')} in {time_diff / 1000000}: OK")

    def test_2(self):
        profile = 'pytest'
        rest = RESTManager(profile=profile)
        cf = CapellaConfigFile(profile)
        org_id = rest.get_capella('/v4/organizations').item(0).key('id')

        start_time = time.perf_counter_ns()
        result = rest.get_capella(f"/v4/organizations/{org_id}/projects").list()
        end_time = time.perf_counter_ns()
        time_diff = end_time - start_time
        print(f"Items: {len(result)} in {time_diff / 1000000}: OK")

        start_time = time.perf_counter_ns()
        result = rest.get_capella(f"/v4/organizations/{org_id}/projects").by_name('pytest-project').unique().id()
        end_time = time.perf_counter_ns()
        time_diff = end_time - start_time
        print(f"Items: {result} in {time_diff / 1000000}: OK")

        start_time = time.perf_counter_ns()
        result = rest.get_capella_kv(f"/v4/organizations/{org_id}/users", "email", cf.account_email).item(0).record()
        end_time = time.perf_counter_ns()
        time_diff = end_time - start_time
        print(f"Item: {result.get('name')} in {time_diff / 1000000}: OK")
