#!/usr/bin/env python3

import os
import re
import time
import pytest
import warnings
from tests.common import start_container, stop_container, run_in_container, cli_run, image_name


warnings.filterwarnings("ignore")
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)


@pytest.mark.serial
class TestCBCCLI(object):
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
        time.sleep(1)

    @classmethod
    def teardown_class(cls):
        print("Stopping test container")
        stop_container(cls.container_id)
        time.sleep(1)

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    def test_cli_1(self, hostname):
        global parent
        cmd = parent + '/tests/test_cli.py'
        args = ['load', '--host', hostname, '--count', '30', '--schema', 'employee_demo', '--replica', '0', '--quota', '128']

        result, output = cli_run(cmd, *args)
        p = re.compile(f"Inserted 30")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    def test_cli_2(self, hostname):
        global parent
        cmd = parent + '/tests/test_cli.py'
        args = ['load', '--host', hostname, '--count', '30', '--schema', 'employee_demo', '--replica', '0', '--quota', '128', '--safe']

        result, output = cli_run(cmd, *args)
        p = re.compile(f"Inserted 0")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    def test_cli_3(self, hostname):
        global parent
        cmd = parent + '/tests/test_cli.py'
        args = ['clean', '--host', hostname, '--schema', 'employee_demo']

        result, output = cli_run(cmd, *args)
        p = re.compile(f"Removing bucket employees")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    def test_cli_4(self, hostname):
        global parent
        cmd = parent + '/tests/test_cli.py'
        args = ['load', '--host', hostname, '--count', '1000', '--schema', 'profile_demo', '--replica', '0']

        result, output = cli_run(cmd, *args)
        p = re.compile(f"Running link rule rule0")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    def test_cli_5(self, hostname):
        global parent
        cmd = parent + '/tests/test_cli.py'
        args = ['clean', '--host', hostname, '--schema', 'profile_demo']

        result, output = cli_run(cmd, *args)
        p = re.compile(f"Removing bucket sample_app")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    def test_cli_6(self, hostname):
        global parent
        cmd = parent + '/tests/test_cli.py'
        args = ['load', '--host', hostname, '--count', '1000', '--schema', 'default', '--replica', '0']

        result, output = cli_run(cmd, *args)
        p = re.compile(f"Processing rules")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    def test_cli_7(self, hostname):
        global parent
        cmd = parent + '/tests/test_cli.py'
        args = ['clean', '--host', hostname, '--schema', 'default']

        result, output = cli_run(cmd, *args)
        p = re.compile(f"Removing bucket cbperf")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    def test_cli_8(self, hostname):
        global parent
        cmd = parent + '/tests/test_cli.py'
        args = ['list', '--host', hostname, '--wait']

        result, output = cli_run(cmd, *args)
        p = re.compile(f"Cluster Host List")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    def test_cli_9(self, hostname):
        global parent
        cmd = parent + '/tests/test_cli.py'
        args = ['load', '--host', hostname, '--count', '100', '--file', current + '/input_template.json', '--replica', '0']

        result, output = cli_run(cmd, *args)
        p = re.compile(f"Processing rules")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    def test_cli_10(self, hostname):
        global parent
        cmd = parent + '/tests/test_cli.py'
        args = ['clean', '--host', hostname]

        result, output = cli_run(cmd, *args)
        p = re.compile(r"Removing bucket pillowfight")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    def test_cli_11(self, hostname):
        global parent
        cmd = parent + '/tests/test_cli.py'
        args = ['load', '--host', hostname]

        result, output = cli_run(cmd, *args, input_file=current + '/input_stdin.dat')
        p = re.compile(r"Collection had 0 documents - inserted 7 additional record")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    def test_cli_12(self, hostname):
        global parent
        cmd = parent + '/tests/test_cli.py'
        args = ['get', '--host', hostname, '-k', 'pillowfight:1']

        result, output = cli_run(cmd, *args)
        p = re.compile(r'"record_id": 1')
        assert p.findall(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    def test_cli_13(self, hostname):
        global parent
        cmd = parent + '/tests/test_cli.py'
        args = ['get', '--host', hostname, '-k', 'pillowfight:%N']

        result, output = cli_run(cmd, *args)
        p = re.compile(r'"record_id": 7')
        assert p.findall(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    def test_cli_14(self, hostname):
        global parent
        cmd = parent + '/tests/test_cli.py'
        args = ['clean', '--host', hostname]

        result, output = cli_run(cmd, *args)
        p = re.compile(r"Removing bucket pillowfight")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    def test_cli_15(self, hostname):
        global parent
        cmd = parent + '/tests/test_cli.py'
        args = ['load', '--host', hostname, '--count', '30', '--schema', 'employee_demo', '--replica', '0']

        result, output = cli_run(cmd, *args)
        p = re.compile(f"Processing rules")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    def test_cli_16(self, hostname):
        global parent
        cmd = parent + '/tests/test_cli.py'
        args = ['export', 'json', '--host', hostname, '-i', '-b', 'employees', '--directory', '/var/tmp']

        result, output = cli_run(cmd, *args)
        p = re.compile(f"Retrieved 30 records")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    def test_cli_17(self, hostname):
        global parent
        cmd = parent + '/tests/test_cli.py'
        args = ['export', 'csv', '--host', hostname, '-i', '-b', 'employees', '--directory', '/var/tmp']

        result, output = cli_run(cmd, *args)
        p = re.compile(f"Retrieved 30 records")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    def test_cli_18(self, hostname):
        global parent
        cmd = parent + '/tests/test_cli.py'
        args = ['clean', '--host', hostname, '--schema', 'employee_demo']

        result, output = cli_run(cmd, *args)
        p = re.compile(f"Removing bucket employees")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    def test_cli_19(self, hostname):
        global parent
        cmd = parent + '/tests/test_cli.py'
        args = ['load', '--host', hostname, '--count', '100', '--schema', 'adjuster_demo', '--replica', '0']

        result, output = cli_run(cmd, *args)
        p = re.compile(f"Processing rules")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    def test_cli_20(self, hostname):
        global parent
        cmd = parent + '/tests/test_cli.py'
        args = ['clean', '--host', hostname, '--schema', 'adjuster_demo']

        result, output = cli_run(cmd, *args)
        p = re.compile(f"Removing bucket adjuster_demo")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    def test_cli_21(self, hostname):
        global parent
        cmd = parent + '/tests/test_cli.py'
        args = ['load', '--host', hostname, '--schema', 'timecard_sample', '--replica', '0']

        result, output = cli_run(cmd, *args)
        p = re.compile(f"Processing rules")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    def test_cli_22(self, hostname):
        global parent
        cmd = parent + '/tests/test_cli.py'
        args = ['clean', '--host', hostname, '--schema', 'timecard_sample']

        result, output = cli_run(cmd, *args)
        p = re.compile(f"Removing bucket timecard_sample")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    def test_cli_23(self, hostname):
        global parent
        cmd = parent + '/tests/test_cli.py'
        args = ['load', '--host', hostname, '--schema', 'insurance_sample', '--replica', '0']

        result, output = cli_run(cmd, *args)
        p = re.compile(f"Processing rules")
        assert p.search(output) is not None
        assert result == 0

    @pytest.mark.parametrize("hostname", ["127.0.0.1"])
    def test_cli_24(self, hostname):
        global parent
        cmd = parent + '/tests/test_cli.py'
        args = ['clean', '--host', hostname, '--schema', 'insurance_sample']

        result, output = cli_run(cmd, *args)
        p = re.compile(f"Removing bucket insurance_sample")
        assert p.search(output) is not None
        assert result == 0
