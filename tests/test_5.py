#!/usr/bin/env python3

import re
import warnings
import pytest
import time
import os
import logging
from tests import get_test_file
from tests.common import start_container, stop_container, run_in_container, cli_run, image_name


warnings.filterwarnings("ignore")
logger = logging.getLogger(__name__)


@pytest.mark.serial
class TestSGWCLI1(object):
    container_id = None

    @classmethod
    def setup_class(cls):
        print("Starting test container")
        platform = f"linux/{os.uname().machine}"
        cls.container_id = start_container(image_name, platform)

        command = ['/bin/bash', '-c', 'test -f /demo/couchbase/.ready']
        while not run_in_container(cls.container_id, command):
            time.sleep(1)

        command = ['cbcutil', 'list', '--host', '127.0.0.1', '--wait']
        run_in_container(cls.container_id, command)

        print("Creating test bucket and loading data")
        command = ['cbcutil', 'load', '--host', '127.0.0.1', '--count', '30', '--schema', 'employee_demo', '--replica', '0', '--safe', '--quota', '128']
        assert run_in_container(cls.container_id, command) is True

    @classmethod
    def teardown_class(cls):
        print("Stopping test container")
        stop_container(cls.container_id)

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    @pytest.mark.parametrize("bucket", ["test"])
    def test_cli_1(self, hostname, bucket):
        cmd = get_test_file('test_sgw_cli.py')
        args = ['database', 'list', '-h', hostname, '-n', "testdb"]

        result, output = cli_run(cmd, *args)
        p = re.compile(f"Database testdb does not exist.")
        logger.debug(f"{cmd} {' '.join(args)}: {output}")
        assert p.search(output) is not None
        assert result == 1

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    @pytest.mark.parametrize("bucket", ["test"])
    def test_cli_2(self, hostname, bucket):
        cmd = get_test_file('test_sgw_cli.py')
        args = ['database', 'create', '-h', hostname, '-n', "testdb", '-b', 'employees']

        result, output = cli_run(cmd, *args)
        p = re.compile(f"Database testdb created for bucket employees")
        logger.debug(f"{cmd} {' '.join(args)}: {output}")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    @pytest.mark.parametrize("bucket", ["test"])
    def test_cli_3(self, hostname, bucket):
        cmd = get_test_file('test_sgw_cli.py')
        args = ['database', 'list', '-h', hostname, '-n', "testdb"]

        result, output = cli_run(cmd, *args)
        logger.debug(f"{cmd} {' '.join(args)}: {output}")
        p = re.compile(f"Bucket:.*employees")
        assert p.search(output) is not None
        p = re.compile(f"Name:.*testdb")
        assert p.search(output) is not None
        p = re.compile(f"Replicas:.*0")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    @pytest.mark.parametrize("bucket", ["test"])
    def test_cli_4(self, hostname, bucket):
        cmd = get_test_file('test_sgw_cli.py')
        args = ['database', 'dump', '-h', hostname, '-n', "testdb"]

        result, output = cli_run(cmd, *args)
        p = re.compile(r"Key: .* Id: .* Channels: .*")
        logger.debug(f"{cmd} {' '.join(args)}: {output}")
        assert p.findall(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    @pytest.mark.parametrize("bucket", ["test"])
    def test_cli_5(self, hostname, bucket):
        cmd = get_test_file('test_sgw_cli.py')
        args = ['user', 'list', '-h', hostname, '-n', "testdb", '--sguser', 'demouser']

        result, output = cli_run(cmd, *args)
        p = re.compile(r"User demouser does not exist")
        logger.debug(f"{cmd} {' '.join(args)}: {output}")
        assert p.search(output) is not None
        assert result == 1

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    @pytest.mark.parametrize("bucket", ["test"])
    def test_cli_6(self, hostname, bucket):
        cmd = get_test_file('test_sgw_cli.py')
        args = ['user', 'create', '-h', hostname, '-n', "testdb", '--sguser', "demouser", '--sgpass', "password"]

        result, output = cli_run(cmd, *args)
        p = re.compile(f"User demouser created for database testdb")
        logger.debug(f"{cmd} {' '.join(args)}: {output}")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    @pytest.mark.parametrize("bucket", ["test"])
    def test_cli_7(self, hostname, bucket):
        cmd = get_test_file('test_sgw_cli.py')
        args = ['user', 'list', '-h', hostname, '-n', "testdb", '--all']

        result, output = cli_run(cmd, *args)
        p = re.compile(f"demouser.*")
        logger.debug(f"{cmd} {' '.join(args)}: {output}")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    @pytest.mark.parametrize("bucket", ["test"])
    def test_cli_8(self, hostname, bucket):
        cmd = get_test_file('test_sgw_cli.py')
        args = ['user', 'list', '-h', hostname, '-n', "testdb", '--sguser', "demouser"]

        result, output = cli_run(cmd, *args)
        logger.debug(f"{cmd} {' '.join(args)}: {output}")
        p = re.compile(f"Name:.*demouser")
        assert p.search(output) is not None
        p = re.compile(f"Admin channels")
        assert p.search(output) is not None
        p = re.compile(f"All channels")
        assert p.search(output) is not None
        p = re.compile(f"Disabled:.*False")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    @pytest.mark.parametrize("bucket", ["test"])
    def test_cli_9(self, hostname, bucket):
        cmd = get_test_file('test_sgw_cli.py')
        args = ['user', 'map', '-h', hostname, '-d', hostname, '-F', 'store_id', '-k', 'employees', '-n', 'testdb']

        result, output = cli_run(cmd, *args)
        logger.debug(f"{cmd} {' '.join(args)}: {output}")
        p = re.compile(r"User store_id@1 created for database testdb")
        assert p.findall(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    @pytest.mark.parametrize("bucket", ["test"])
    def test_cli_10(self, hostname, bucket):
        cmd = get_test_file('test_sgw_cli.py')
        args = ['user', 'list', '-h', hostname, '-n', "testdb", '--sguser', "store_id@1"]

        result, output = cli_run(cmd, *args)
        logger.debug(f"{cmd} {' '.join(args)}: {output}")
        p = re.compile(r"Name:.*store_id@1")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    @pytest.mark.parametrize("bucket", ["test"])
    def test_cli_11(self, hostname, bucket):
        cmd = get_test_file('test_sgw_cli.py')
        args = ['database', 'sync', '-h', hostname, '-n', 'testdb', '-f', get_test_file('employee.js')]

        result, output = cli_run(cmd, *args)
        logger.debug(f"{cmd} {' '.join(args)}: {output}")
        p = re.compile(f"Sync function created for database testdb")
        assert p.findall(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    @pytest.mark.parametrize("bucket", ["test"])
    def test_cli_12(self, hostname, bucket):
        cmd = get_test_file('test_sgw_cli.py')
        args = ['database', 'sync', '-h', hostname, '-n', 'testdb', '-g']

        result, output = cli_run(cmd, *args)
        logger.debug(f"{cmd} {' '.join(args)}: {output}")
        p = re.compile(r"function sync.*")
        assert p.findall(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    @pytest.mark.parametrize("bucket", ["test"])
    def test_cli_13(self, hostname, bucket):
        cmd = get_test_file('test_sgw_cli.py')
        args = ['user', 'delete', '-h', hostname, '-n', "testdb", '--sguser', "demouser"]

        result, output = cli_run(cmd, *args)
        logger.debug(f"{cmd} {' '.join(args)}: {output}")
        p = re.compile(f"User demouser deleted from testdb")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    @pytest.mark.parametrize("bucket", ["test"])
    def test_cli_14(self, hostname, bucket):
        cmd = get_test_file('test_sgw_cli.py')
        args = ['database', 'delete', '-h', hostname, '-n', "testdb"]

        result, output = cli_run(cmd, *args)
        logger.debug(f"{cmd} {' '.join(args)}: {output}")
        p = re.compile(f"Database testdb deleted")
        assert p.search(output) is not None
        assert result == 0


@pytest.mark.serial
class TestSGWCLI2(object):
    container_id = None

    @classmethod
    def setup_class(cls):
        print("Starting test container")
        platform = f"linux/{os.uname().machine}"
        cls.container_id = start_container(image_name, platform)

        command = ['/bin/bash', '-c', 'test -f /demo/couchbase/.ready']
        while not run_in_container(cls.container_id, command):
            time.sleep(1)

        command = ['cbcutil', 'list', '--host', '127.0.0.1', '--wait']
        run_in_container(cls.container_id, command)

        print("Creating test bucket and loading data")
        command = ['cbcutil', 'load', '--host', '127.0.0.1', '--schema', 'insurance_sample', '--replica', '0', '--safe', '--quota', '128']
        assert run_in_container(cls.container_id, command) is True

    @classmethod
    def teardown_class(cls):
        print("Stopping test container")
        stop_container(cls.container_id)

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    @pytest.mark.parametrize("bucket", ["test"])
    def test_cli_1(self, hostname, bucket):
        cmd = get_test_file('test_sgw_cli.py')
        args = ['database', 'create', '-h', hostname, '-n', 'insurance', '-b', 'insurance_sample', '-k', 'insurance_sample.data']

        result, output = cli_run(cmd, *args)
        logger.debug(f"{cmd} {' '.join(args)}: {output}")
        p = re.compile(f"Database insurance created")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    @pytest.mark.parametrize("bucket", ["test"])
    def test_cli_2(self, hostname, bucket):
        cmd = get_test_file('test_sgw_cli.py')
        args = ['user', 'map', '-h', hostname, '-d', hostname, '-F', 'region', '-k', 'insurance_sample', '-n', 'insurance']

        result, output = cli_run(cmd, *args)
        logger.debug(f"{cmd} {' '.join(args)}: {output}")
        p = re.compile(r"User region@global created for database insurance")
        assert p.findall(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    @pytest.mark.parametrize("bucket", ["test"])
    def test_cli_3(self, hostname, bucket):
        cmd = get_test_file('test_sgw_cli.py')
        args = ['database', 'sync', '-h', hostname, '-n', 'insurance', '-f', get_test_file('insurance.js')]

        result, output = cli_run(cmd, *args)
        logger.debug(f"{cmd} {' '.join(args)}: {output}")
        p = re.compile(f"Sync function created for database insurance.data.adjuster")
        assert p.findall(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    @pytest.mark.parametrize("bucket", ["test"])
    def test_cli_4(self, hostname, bucket):
        cmd = get_test_file('test_sgw_cli.py')
        args = ['auth', 'session', '-h', hostname, '-n', 'insurance', '-U', 'region@central']

        result, output = cli_run(cmd, *args)
        logger.debug(f"{cmd} {' '.join(args)}: {output}")
        p = re.compile(f".*cookie_name.*SyncGatewaySession")
        assert p.findall(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    @pytest.mark.parametrize("bucket", ["test"])
    def test_cli_5(self, hostname, bucket):
        cmd = get_test_file('test_sgw_cli.py')
        args = ['database', 'delete', '-h', hostname, '-n', "insurance"]

        result, output = cli_run(cmd, *args)
        logger.debug(f"{cmd} {' '.join(args)}: {output}")
        p = re.compile(f"Database insurance deleted")
        assert p.search(output) is not None
        assert result == 0
