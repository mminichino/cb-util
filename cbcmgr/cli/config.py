##
##

import logging
import os
import warnings
import argparse
from enum import Enum
from cbcmgr.cli.schema import ProcessSchema, ProcessVariables
from cbcmgr import get_config_file

warnings.filterwarnings("ignore")


class OperatingMode(Enum):
    LOAD = 0
    READ = 1
    LIST = 2


if 'HOME' in os.environ:
    home_dir = os.environ['HOME']
else:
    home_dir = '/var/tmp'

schema_file = get_config_file("schema.json")
username = "Administrator"
password = "password"
tls = False
host = "localhost"
external_network = False
default_debug_file = os.path.join(home_dir, "cb_debug.log")
debug_file = os.environ.get("CB_PERF_DEBUG_FILE", default_debug_file)
schema_name = None
input_file = None
inventory = None
schema = None
output_file = None
output_dir = None
command = "load"
op_mode = OperatingMode.LOAD.value
continuous = False
batch_size = 100
count = 100
replicas = 0
bucket_quota = 256
bucket_name = None
scope_name = None
collection_name = None
document_key = None
capella_project = None
capella_db = None
insert_data = None
wait_mode = False
ping_mode = False
test_mode = False
safe_mode = False
schema_file_json = {}
id_key = None
quiet_mode = False
create_indexes = False
screen_output = False
key_field = None
plugin_name = None
plugin_vars = {}


def process_params(parameters: argparse.Namespace) -> None:
    logger = logging.getLogger(__name__)
    global username, \
        password, \
        tls, \
        host, \
        external_network, \
        schema, \
        input_file, \
        schema_name, \
        inventory, \
        output_file, \
        output_dir, \
        command, \
        op_mode, \
        count, \
        replicas, \
        bucket_quota, \
        bucket_name, \
        scope_name, \
        collection_name, \
        document_key, \
        capella_project, \
        capella_db, \
        insert_data, \
        wait_mode, \
        ping_mode, \
        test_mode, \
        safe_mode, \
        schema_file_json, \
        id_key, \
        quiet_mode, \
        create_indexes, \
        screen_output, \
        key_field, \
        plugin_name, \
        plugin_vars

    if parameters.user:
        username = parameters.user
    if parameters.password:
        password = parameters.password
    if parameters.tls:
        tls = parameters.tls
    if parameters.host:
        host = parameters.host
    if parameters.external:
        external_network = parameters.external
    if parameters.file:
        input_file = parameters.file
        schema_file_json = ProcessVariables().read_file(input_file)
    if parameters.id:
        id_key = parameters.id
    if parameters.outfile:
        output_file = parameters.outfile
    if parameters.replica:
        replicas = parameters.replica
    if parameters.quota:
        bucket_quota = parameters.quota
    if parameters.bucket:
        bucket_name = parameters.bucket
    if parameters.scope:
        scope_name = parameters.scope
    if parameters.collection:
        collection_name = parameters.collection
    if parameters.key:
        document_key = parameters.key
    if parameters.project:
        capella_project = parameters.project
    if parameters.db:
        capella_db = parameters.db
    if parameters.data:
        insert_data = parameters.data
    if parameters.quiet:
        quiet_mode = parameters.quiet
    if parameters.index:
        create_indexes = parameters.index
    if parameters.stdout:
        screen_output = parameters.stdout
    if parameters.safe:
        safe_mode = parameters.safe
    if parameters.variable:
        for variable in parameters.variable:
            key = variable.split('=')[0]
            value = '='.join(variable.split('=')[1:])
            logger.debug(f"Adding plugin variable {key}:{value}")
            plugin_vars.update({key: value})
    if parameters.docid:
        key_field = parameters.docid
    if parameters.plugin:
        plugin_name = parameters.plugin
    if parameters.directory:
        output_dir = parameters.directory
    else:
        output_dir = os.environ['HOME']
    if input_file:
        schema_name = "external_file"
        parameters.schema = "external_file"
    else:
        if parameters.schema:
            schema_name = parameters.schema
    if parameters.command:
        command = parameters.command
    if command == 'load':
        op_mode = OperatingMode.LOAD.value
    elif command == "get":
        op_mode = OperatingMode.READ.value
    elif command == "list":
        op_mode = OperatingMode.LIST.value
    if parameters.count:
        count = parameters.count

    if op_mode == OperatingMode.LIST.value:
        if parameters.wait:
            wait_mode = parameters.wait
        if parameters.ping:
            ping_mode = parameters.ping
        if parameters.test:
            test_mode = parameters.test

    if schema_name:
        inventory = ProcessSchema(schema_file).inventory()
        schema = inventory.get(schema_name)
