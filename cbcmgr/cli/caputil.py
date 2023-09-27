##
##

import argparse
import warnings
from overrides import override
from cbcmgr import VERSION
from cbcmgr.cli.cli import CLI
from cbcmgr.cli.exceptions import *
from cbcmgr.cb_capella import Capella
import pandas as pd

warnings.filterwarnings("ignore")
logger = logging.getLogger()


class CapellaCLI(CLI):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @override()
    def local_args(self):
        opt_parser = argparse.ArgumentParser(parents=[self.parser], add_help=False)
        opt_parser.add_argument('-n', '--name', action='store', help="Object Name")
        opt_parser.add_argument('-p', '--project', action='store', help="Project Name")

        command_subparser = self.parser.add_subparsers(dest='command')
        cluster_parser = command_subparser.add_parser('cluster', help="Cluster Operations", parents=[opt_parser], add_help=False)
        cluster_subparser = cluster_parser.add_subparsers(dest='cluster_command')
        cluster_subparser.add_parser('get', help="Export CSV", parents=[opt_parser], add_help=False)
        cluster_subparser.add_parser('list', help="Export JSON", parents=[opt_parser], add_help=False)
        project_parser = command_subparser.add_parser('project', help="Cluster Operations", parents=[opt_parser], add_help=False)
        project_subparser = project_parser.add_subparsers(dest='project_command')
        project_subparser.add_parser('get', help="Export CSV", parents=[opt_parser], add_help=False)
        project_subparser.add_parser('list', help="Export JSON", parents=[opt_parser], add_help=False)
        org_parser = command_subparser.add_parser('org', help="Cluster Operations", parents=[opt_parser], add_help=False)
        org_subparser = org_parser.add_subparsers(dest='org_command')
        org_subparser.add_parser('get', help="Export CSV", parents=[opt_parser], add_help=False)
        org_subparser.add_parser('list', help="Export JSON", parents=[opt_parser], add_help=False)

    def run(self):
        logger.info("CapUtil version %s" % VERSION)

        if self.options.command == 'cluster':
            cm = Capella()
            project = cm.get_project(self.options.project)
            if project:
                project_id = project.get('id')
            else:
                logger.error(f"Can not find project {self.options.project}")
                return

            pm = Capella(project_id=project_id)
            data = pm.list_clusters()
            df = pd.json_normalize(data)
            dx = [pd.json_normalize(s) for s in df['serviceGroups']]
            for idx, data in enumerate(dx):
                data['id'] = df.iloc[idx]['id']
                data['name'] = df.iloc[idx]['name']
                data['currentState'] = df.iloc[idx]['currentState']
                data['cloud'] = df.iloc[idx]['cloudProvider.type']
                data['region'] = df.iloc[idx]['cloudProvider.region']
                data['version'] = df.iloc[idx]['couchbaseServer.version']
            subset_df = pd.concat(dx).reset_index(drop=True)

            if self.options.cluster_command == "get":
                result = pd.DataFrame(subset_df[(subset_df.name == self.options.name)])
                if not result.empty:
                    print(result)
            elif self.options.cluster_command == "list":
                print(pd.DataFrame(subset_df).to_string())
        elif self.options.command == 'project':
            cm = Capella()
            data = cm.list_projects()
            df = pd.json_normalize(data)
            subset_df = df[["id", "name", "audit.createdAt", "description"]]

            if self.options.project_command == "get":
                result = pd.DataFrame(subset_df[(subset_df.name == self.options.name)])
                if not result.empty:
                    print(result)
            elif self.options.project_command == "list":
                print(pd.DataFrame(subset_df).to_string())
        elif self.options.command == 'org':
            cm = Capella()
            data = cm.list_organizations()
            df = pd.json_normalize(data)
            subset_df = df[["id", "name", "audit.createdAt", "preferences.sessionDuration"]]

            if self.options.org_command == "get":
                result = pd.DataFrame(subset_df[(subset_df.name == self.options.name)])
                if not result.empty:
                    print(result)
            elif self.options.org_command == "list":
                print(pd.DataFrame(subset_df).to_string())


def main(args=None):
    cli = CapellaCLI(args)
    cli.run()
    sys.exit(0)
