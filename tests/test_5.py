#!/usr/bin/env python3

import subprocess
import re
import warnings
import pytest
import docker
from tests import get_test_file

warnings.filterwarnings("ignore")


def cli_run(cmd: str, *args: str):
    command_output = ""
    run_cmd = [
        cmd,
        *args
    ]

    p = subprocess.Popen(run_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    while True:
        line = p.stdout.readline()
        if not line:
            break
        line_string = line.decode("utf-8")
        command_output += line_string

    p.communicate()

    return p.returncode, command_output


@pytest.fixture(scope="module", autouse=True)
def data_load(request):
    client = docker.from_env()
    container_id = client.containers.get('pytest')
    print("Creating test buckets and loading data")
    exit_code, output = container_id.exec_run(['cbcutil',
                                               'load',
                                               '--host', '127.0.0.1',
                                               '--count', '30',
                                               '--schema', 'employee_demo',
                                               '--replica', '0',
                                               '--safe',
                                               '--quota', '128'])
    for line in output.split(b'\n'):
        print(line.decode("utf-8"))
    assert exit_code == 0
    exit_code, output = container_id.exec_run(['cbcutil',
                                               'load',
                                               '--host', '127.0.0.1',
                                               '--schema', 'insurance_sample',
                                               '--replica', '0',
                                               '--safe',
                                               '--quota', '128'])
    for line in output.split(b'\n'):
        print(line.decode("utf-8"))
    assert exit_code == 0
    print("Ready.")

    yield exit_code

    print("Removing test buckets")
    exit_code, output = container_id.exec_run(['cbcutil',
                                               'clean',
                                               '--host', '127.0.0.1',
                                               '--schema', 'employee_demo'])
    for line in output.split(b'\n'):
        print(line.decode("utf-8"))
    assert exit_code == 0
    exit_code, output = container_id.exec_run(['cbcutil',
                                               'clean',
                                               '--host', '127.0.0.1',
                                               '--schema', 'insurance_sample'])
    for line in output.split(b'\n'):
        print(line.decode("utf-8"))
    assert exit_code == 0
    print("Test Complete")


def test_cli_1(hostname, bucket):
    cmd = get_test_file('test_sgw_cli.py')
    args = ['database', 'list', '-h', hostname, '-n', "testdb"]

    result, output = cli_run(cmd, *args)
    p = re.compile(f"Database testdb does not exist.")
    assert p.search(output) is not None
    assert result == 1


def test_cli_2(hostname, bucket):
    cmd = get_test_file('test_sgw_cli.py')
    args = ['database', 'create', '-h', hostname, '-n', "testdb", '-b', 'employees']

    result, output = cli_run(cmd, *args)
    p = re.compile(f"Database testdb created for bucket employees")
    assert p.search(output) is not None
    assert result == 0


def test_cli_3(hostname, bucket):
    cmd = get_test_file('test_sgw_cli.py')
    args = ['database', 'list', '-h', hostname, '-n', "testdb"]

    result, output = cli_run(cmd, *args)
    p = re.compile(f"Bucket:.*employees")
    assert p.search(output) is not None
    p = re.compile(f"Name:.*testdb")
    assert p.search(output) is not None
    p = re.compile(f"Replicas:.*0")
    assert p.search(output) is not None
    assert result == 0


def test_cli_4(hostname, bucket):
    cmd = get_test_file('test_sgw_cli.py')
    args = ['database', 'dump', '-h', hostname, '-n', "testdb"]

    result, output = cli_run(cmd, *args)
    p = re.compile(r"Key: .* Id: .* Channels: .*")
    assert p.findall(output) is not None
    assert result == 0


def test_cli_5(hostname, bucket):
    cmd = get_test_file('test_sgw_cli.py')
    args = ['user', 'list', '-h', hostname, '-n', "testdb", '--sguser', 'demouser']

    result, output = cli_run(cmd, *args)
    p = re.compile(r"User demouser does not exist")
    assert p.search(output) is not None
    assert result == 1


def test_cli_6(hostname, bucket):
    cmd = get_test_file('test_sgw_cli.py')
    args = ['user', 'create', '-h', hostname, '-n', "testdb", '--sguser', "demouser", '--sgpass', "password"]

    result, output = cli_run(cmd, *args)
    p = re.compile(f"User demouser created for database testdb")
    assert p.search(output) is not None
    assert result == 0


def test_cli_7(hostname, bucket):
    cmd = get_test_file('test_sgw_cli.py')
    args = ['user', 'list', '-h', hostname, '-n', "testdb", '--all']

    result, output = cli_run(cmd, *args)
    p = re.compile(f"demouser.*")
    assert p.search(output) is not None
    assert result == 0


def test_cli_8(hostname, bucket):
    cmd = get_test_file('test_sgw_cli.py')
    args = ['user', 'list', '-h', hostname, '-n', "testdb", '--sguser', "demouser"]

    result, output = cli_run(cmd, *args)
    p = re.compile(f"Name:.*demouser")
    assert p.search(output) is not None
    p = re.compile(f"Admin channels")
    assert p.search(output) is not None
    p = re.compile(f"All channels")
    assert p.search(output) is not None
    p = re.compile(f"Disabled:.*False")
    assert p.search(output) is not None
    assert result == 0


def test_cli_9(hostname, bucket):
    cmd = get_test_file('test_sgw_cli.py')
    args = ['user', 'map', '-h', hostname, '-d', hostname, '-F', 'store_id', '-k', 'employees', '-n', 'testdb']

    result, output = cli_run(cmd, *args)
    p = re.compile(r"User store_id@1 created for database testdb")
    assert p.findall(output) is not None
    assert result == 0


def test_cli_10(hostname, bucket):
    cmd = get_test_file('test_sgw_cli.py')
    args = ['user', 'list', '-h', hostname, '-n', "testdb", '--sguser', "store_id@1"]

    result, output = cli_run(cmd, *args)
    p = re.compile(r"Name:.*store_id@1")
    assert p.search(output) is not None
    assert result == 0


def test_cli_11(hostname, bucket):
    cmd = get_test_file('test_sgw_cli.py')
    args = ['database', 'sync', '-h', hostname, '-n', 'testdb', '-f', get_test_file('employee.js')]

    result, output = cli_run(cmd, *args)
    p = re.compile(f"Sync function created for database testdb")
    assert p.findall(output) is not None
    assert result == 0


def test_cli_12(hostname, bucket):
    cmd = get_test_file('test_sgw_cli.py')
    args = ['database', 'sync', '-h', hostname, '-n', 'testdb', '-g']

    result, output = cli_run(cmd, *args)
    p = re.compile(r"function sync.*")
    assert p.findall(output) is not None
    assert result == 0


def test_cli_13(hostname, bucket):
    cmd = get_test_file('test_sgw_cli.py')
    args = ['user', 'delete', '-h', hostname, '-n', "testdb", '--sguser', "demouser"]

    result, output = cli_run(cmd, *args)
    p = re.compile(f"User demouser deleted from testdb")
    assert p.search(output) is not None
    assert result == 0


def test_cli_14(hostname, bucket):
    cmd = get_test_file('test_sgw_cli.py')
    args = ['database', 'delete', '-h', hostname, '-n', "testdb"]

    result, output = cli_run(cmd, *args)
    p = re.compile(f"Database testdb deleted")
    assert p.search(output) is not None
    assert result == 0


def test_cli_15(hostname, bucket):
    cmd = get_test_file('test_sgw_cli.py')
    args = ['database', 'create', '-h', hostname, '-n', 'insurance', '-b', 'insurance_sample', '-k', 'insurance_sample.data']

    result, output = cli_run(cmd, *args)
    p = re.compile(f"Database insurance created")
    assert p.search(output) is not None
    assert result == 0


def test_cli_16(hostname, bucket):
    cmd = get_test_file('test_sgw_cli.py')
    args = ['user', 'map', '-h', hostname, '-d', hostname, '-F', 'region', '-k', 'insurance_sample', '-n', 'insurance']

    result, output = cli_run(cmd, *args)
    p = re.compile(r"User region@global created for database insurance")
    assert p.findall(output) is not None
    assert result == 0


def test_cli_17(hostname, bucket):
    cmd = get_test_file('test_sgw_cli.py')
    args = ['database', 'sync', '-h', hostname, '-n', 'insurance', '-f', get_test_file('insurance.js')]

    result, output = cli_run(cmd, *args)
    p = re.compile(f"Sync function created for database insurance.data.adjuster")
    assert p.findall(output) is not None
    assert result == 0


def test_cli_18(hostname, bucket):
    cmd = get_test_file('test_sgw_cli.py')
    args = ['auth', 'session', '-h', hostname, '-n', 'insurance', '-U', 'region@central']

    result, output = cli_run(cmd, *args)
    p = re.compile(f".*cookie_name.*SyncGatewaySession")
    assert p.findall(output) is not None
    assert result == 0


def test_cli_19(hostname, bucket):
    cmd = get_test_file('test_sgw_cli.py')
    args = ['database', 'delete', '-h', hostname, '-n', "insurance"]

    result, output = cli_run(cmd, *args)
    p = re.compile(f"Database insurance deleted")
    assert p.search(output) is not None
    assert result == 0
