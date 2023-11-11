##
##

import argparse
import warnings
from overrides import override
from cbcmgr import VERSION
from cbcmgr.cli.cli import CLI
from cbcmgr.cli.exceptions import *
import cbcmgr.cli.config as config
from cbcmgr.cli.export import CBExport, ExportType
from cbcmgr.cli.pimport import PluginImport
from cbcmgr.cli.main import MainLoop
from cbcmgr.cli.replicate import Replicator
from cbcmgr.cli.config import OperatingMode


LOAD_DATA = 0x0000
KV_TEST = 0x0001
QUERY_TEST = 0x0002
REMOVE_DATA = 0x0003
PAUSE_TEST = 0x0009
INSTANCE_MAX = 0x200
RUN_STOP = 0xFFFF

warnings.filterwarnings("ignore")
logger = logging.getLogger()


def int_arg(value):
    try:
        return int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("numeric argument expected")


class CBCUtil(CLI):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @override()
    def local_args(self):
        opt_parser = argparse.ArgumentParser(parents=[self.parser], add_help=False)
        opt_parser.add_argument('-u', '--user', action='store', help="User Name", default="Administrator")
        opt_parser.add_argument('-p', '--password', action='store', help="User Password", default="password")
        opt_parser.add_argument('-h', '--host', action='store', help="Cluster Node Name", default="localhost")
        opt_parser.add_argument('-b', '--bucket', action='store', help="Bucket", default="pillowfight")
        opt_parser.add_argument('-s', '--scope', action='store', help="Scope", default="_default")
        opt_parser.add_argument('-c', '--collection', action='store', help="Collection", default="_default")
        opt_parser.add_argument('-k', '--key', action='store', help="Document Key")
        opt_parser.add_argument('-d', '--data', action='store', help="Document To Insert")
        opt_parser.add_argument('-q', '--quiet', action='store_true', help="Suppress Output")
        opt_parser.add_argument('-i', '--index', action='store_true', help="Create Index")
        opt_parser.add_argument('-O', '--stdout', action='store_true', help="Output to terminal")
        opt_parser.add_argument('-P', '--plugin', action='store', help="Export Plugin")
        opt_parser.add_argument('-V', '--variable', action='append', help="Plugin Variables")
        opt_parser.add_argument('-F', '--filter', nargs='+', action='extend', help="Filter Expressions")
        opt_parser.add_argument('--project', action='store', help="Capella project")
        opt_parser.add_argument('--db', action='store', help="Capella database")
        opt_parser.add_argument('--docid', action='store', help="Import document ID field", default="doc_id")
        opt_parser.add_argument('--tls', action='store_true', help="Enable SSL")
        opt_parser.add_argument('--safe', action='store_true', help="Do not overwrite data")
        opt_parser.add_argument('--defer', action='store_true', help="Defer index build")
        opt_parser.add_argument('-e', '--external', action='store_true', help='Use external network')
        opt_parser.add_argument('-f', '--file', action='store', help="File based collection schema JSON")
        opt_parser.add_argument('--outfile', action='store', help="Output file", default="output.dat")
        opt_parser.add_argument('--directory', action='store', help="Output directory")
        opt_parser.add_argument('--schema', action='store', help="Test Schema")
        opt_parser.add_argument('--count', action='store', help="Record Count", type=int_arg)
        opt_parser.add_argument('--replica', action='store', help="Replica Count", type=int_arg, default=1)
        opt_parser.add_argument('--quota', action='store', help="Bucket Memory Quota", type=int_arg)
        opt_parser.add_argument('--id', action='store', help="ID field for file based collection schema", default="record_id")
        opt_parser.add_argument('--ping', action='store_true', help='Show cluster ping output')
        opt_parser.add_argument('--test', action='store_true', help='Just check status and error if not ready')
        opt_parser.add_argument('--wait', action='store_true', help='Wait for cluster to be ready')

        command_subparser = self.parser.add_subparsers(dest='command')
        list_parser = command_subparser.add_parser('list', help="List Nodes", parents=[opt_parser], add_help=False)
        list_subparser = list_parser.add_subparsers(dest='list_command')
        list_subparser.add_parser('quota', help="Show quotas", parents=[opt_parser], add_help=False)
        command_subparser.add_parser('clean', help="Clean Up", parents=[opt_parser], add_help=False)
        command_subparser.add_parser('load', help="Load Data", parents=[opt_parser], add_help=False)
        command_subparser.add_parser('get', help="Get Data", parents=[opt_parser], add_help=False)
        command_subparser.add_parser('schema', help="Schema Admin", parents=[opt_parser], add_help=False)
        command_subparser.add_parser('import', help="Import Data", parents=[opt_parser], add_help=False)
        export_parser = command_subparser.add_parser('export', help="Export Data", parents=[opt_parser], add_help=False)
        export_subparser = export_parser.add_subparsers(dest='export_command')
        export_subparser.add_parser('csv', help="Export CSV", parents=[opt_parser], add_help=False)
        export_subparser.add_parser('json', help="Export JSON", parents=[opt_parser], add_help=False)
        replicate_parser = command_subparser.add_parser('replicate', help="Replicate Data", parents=[opt_parser], add_help=False)
        replicate_subparser = replicate_parser.add_subparsers(dest='replicate_command')
        replicate_subparser.add_parser('source', help="Source Side", parents=[opt_parser], add_help=False)
        replicate_subparser.add_parser('target', help="Target Side", parents=[opt_parser], add_help=False)

    def run(self):
        if 'replicate_command' in self.options and self.options.replicate_command != 'source':
            logger.info("CBCUtil version %s" % VERSION)

        config.process_params(self.options)

        if self.options.command == 'list':
            if self.options.list_command == 'quota':
                MainLoop().display_quota_settings()
            else:
                MainLoop().cluster_list()
        elif self.options.command == 'schema':
            MainLoop().schema_list()
        elif self.options.command == 'clean':
            MainLoop().schema_remove()
        elif self.options.command == 'export':
            if self.options.export_command == 'csv':
                CBExport().export(ExportType.csv)
            elif self.options.export_command == 'json':
                CBExport().export(ExportType.json)
        elif self.options.command == 'import':
            PluginImport().import_tables()
        elif self.options.command == 'replicate':
            if self.options.replicate_command == 'source':
                Replicator(self.options.filter).source()
            elif self.options.replicate_command == 'target':
                Replicator(deferred=self.options.defer).target()
        else:
            if config.op_mode == OperatingMode.LOAD.value and self.options.schema:
                MainLoop().schema_load()
            elif config.op_mode == OperatingMode.LOAD.value:
                MainLoop().input_load()
            elif config.op_mode == OperatingMode.READ.value:
                MainLoop().read()


def main(args=None):
    cli = CBCUtil(args)
    cli.run()
    sys.exit(0)
