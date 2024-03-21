##

import docker
from docker.errors import APIError
from docker.models.containers import Container
from docker import APIClient
from typing import Union, List
from io import BytesIO
import io
import os
import tarfile
import warnings
import logging
import subprocess

warnings.filterwarnings("ignore")
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
logging.getLogger("docker").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

image_name = "mminichino/cbdev:4.0.2"
document = {
    "id": 1,
    "data": "data",
    "one": "one",
    "two": "two",
    "three": "tree"
}
new_document = {
    "id": 1,
    "data": "new",
    "one": "one",
    "two": "two",
    "three": "tree"
}
query_result = [
    {
        'data': 'data'
    }
]

json_data = {
    "name": "John Doe",
    "email": "jdoe@example.com",
    "addresses": {
        "billing": {
            "line1": "123 Any Street",
            "line2": "Anywhere",
            "country": "United States"
        },
        "delivery": {
            "line1": "123 Any Street",
            "line2": "Anywhere",
            "country": "United States"
        }
    },
    "history": {
        "events": [
            {
                "event_id": "1",
                "date": "1/1/1970",
                "type": "contact"
            },
            {
                "event_id": "2",
                "date": "1/1/1970",
                "type": "contact"
            }
        ]
    },
    "purchases": {
        "complete": [
            339, 976, 442, 777
        ],
        "abandoned": [
            157, 42, 999
        ]
    }
}

xml_data = """<?xml version="1.0" encoding="UTF-8" ?>
 <root>
   <name>John Doe</name>
   <email>jdoe@example.com</email>
   <addresses>
     <billing>
       <line1>123 Any Street</line1>
       <line2>Anywhere</line2>
       <country>United States</country>
     </billing>
     <delivery>
       <line1>123 Any Street</line1>
       <line2>Anywhere</line2>
       <country>United States</country>
     </delivery>
   </addresses>
   <history>
     <events>
       <event_id>1</event_id>
       <date>1/1/1970</date>
       <type>contact</type>
     </events>
     <events>
       <event_id>2</event_id>
       <date>1/1/1970</date>
       <type>contact</type>
     </events>
   </history>
   <purchases>
     <complete>339</complete>
     <complete>976</complete>
     <complete>442</complete>
     <complete>777</complete>
     <abandoned>157</abandoned>
     <abandoned>42</abandoned>
     <abandoned>999</abandoned>
   </purchases>
 </root>
 """


def make_local_dir(name: str):
    if not os.path.exists(name):
        path_dir = os.path.dirname(name)
        if not os.path.exists(path_dir):
            make_local_dir(path_dir)
        try:
            os.mkdir(name)
        except OSError:
            raise


def cmd_exec(command: Union[str, List[str]], directory: str):
    buffer = io.BytesIO()

    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=directory)

    while True:
        data = p.stdout.read()
        if not data:
            break
        buffer.write(data)

    p.communicate()

    if p.returncode != 0:
        raise ValueError("command exited with non-zero return code")

    buffer.seek(0)
    return buffer


def cli_run(cmd: str, *args: str, input_file: str = None):
    command_output = ""
    run_cmd = [
        cmd,
        *args
    ]

    p = subprocess.Popen(run_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    if input_file:
        with open(input_file, 'rb') as input_data:
            while True:
                line = input_data.readline()
                if not line:
                    break
                p.stdin.write(line)
            p.stdin.close()

    while True:
        line = p.stdout.readline()
        if not line:
            break
        line_string = line.decode("utf-8")
        command_output += line_string

    p.wait()

    return p.returncode, command_output


def copy_to_container(container_id: Container, src: str, dst: str):
    print(f"Copying {src} to {dst}")
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode='w|') as tar, open(src, 'rb') as file:
        info = tar.gettarinfo(fileobj=file)
        info.name = os.path.basename(src)
        tar.addfile(info, file)

    container_id.put_archive(dst, stream.getvalue())


def copy_log_from_container(container_id: Container, src: str, directory: str):
    make_local_dir(directory)
    src_base = os.path.basename(src)
    dst = f"{directory}/{src_base}"
    print(f"Copying {src} to {dst}")
    try:
        bits, stat = container_id.get_archive(src)
    except docker.errors.NotFound:
        print(f"{src}: not found")
        return
    stream = io.BytesIO()
    for chunk in bits:
        stream.write(chunk)
    stream.seek(0)
    with tarfile.open(fileobj=stream, mode='r') as tar, open(dst, 'wb') as file:
        f = tar.extractfile(src_base)
        data = f.read()
        file.write(data)


def copy_dir_to_container(container_id: Container, src_dir: str, dst: str):
    print(f"Copying {src_dir} to {dst}")
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode='w|') as tar:
        name = os.path.basename(src_dir)
        tar.add(src_dir, arcname=name, recursive=True)

    container_id.put_archive(dst, stream.getvalue())


def copy_git_to_container(container_id: Container, src: str, dst: str):
    container_mkdir(container_id, dst)
    file_list = []
    print(f"Copying git HEAD to {dst}")
    output: BytesIO = cmd_exec(["git", "ls-tree", "--full-tree", "--name-only", "-r", "HEAD"], src)
    while True:
        line = output.readline()
        if not line:
            break
        line_string = line.decode("utf-8")
        file_list.append(line_string.strip())
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode='w|') as tar:
        for filename in file_list:
            tar.add(filename, recursive=True)

    container_id.put_archive(dst, stream.getvalue())


def container_mkdir(container_id: Container, directory: str):
    command = ["mkdir", "-p", directory]
    exit_code, output = container_id.exec_run(command)
    assert exit_code == 0


def start_container(image: str, platform: str = "linux/amd64") -> Container:
    docker_api = APIClient(base_url='unix://var/run/docker.sock')
    client = docker.from_env()
    client.images.prune()
    client.containers.prune()
    client.networks.prune()
    client.volumes.prune()
    docker_api.prune_builds()

    print(f"Starting {image}")

    try:
        container_id = client.containers.run(image,
                                             tty=True,
                                             detach=True,
                                             platform=platform,
                                             name="pytest",
                                             ports={
                                                8091: 8091,
                                                18091: 18091,
                                                8092: 8092,
                                                18092: 18092,
                                                8093: 8093,
                                                18093: 18093,
                                                8094: 8094,
                                                18094: 18094,
                                                8095: 8095,
                                                18095: 18095,
                                                8096: 8096,
                                                18096: 18096,
                                                8097: 8097,
                                                18097: 18097,
                                                11207: 11207,
                                                11210: 11210,
                                                9102: 9102,
                                                4984: 4984,
                                                4985: 4985,
                                             },
                                             )
    except docker.errors.APIError as e:
        if e.status_code == 409:
            container_id = client.containers.get('pytest')
        else:
            raise

    print("Container started")
    return container_id


def get_image_name(container_id: Container):
    tags = container_id.image.tags
    return tags[0].split(':')[0].replace('/', '-')


def container_log(container_id: Container, directory: str):
    make_local_dir(directory)
    print(f"Copying {container_id.name} log to {directory}")
    filename = f"{directory}/{container_id.name}.log"
    output = container_id.attach(stdout=True, stderr=True, logs=True)
    with open(filename, 'w') as out_file:
        out_file.write(output.decode("utf-8"))
        out_file.close()


def run_in_container(container_id: Container, command: Union[str, List[str]], directory: Union[str, None] = None):
    exit_code, output = container_id.exec_run(command, workdir=directory)
    for line in output.split(b'\n'):
        if len(line) > 0:
            print(line.decode("utf-8"))
    if exit_code == 0:
        return True
    else:
        return False


def get_container_id(name: str = "pytest"):
    client = docker.from_env()
    try:
        return client.containers.get(name)
    except docker.errors.NotFound:
        return None


def stop_container(container_id: Container):
    client = docker.from_env()
    container_id.stop()
    container_id.remove()
    try:
        volume = client.volumes.get("pytest-volume")
        volume.remove()
    except docker.errors.NotFound:
        pass
    print("Container stopped")
