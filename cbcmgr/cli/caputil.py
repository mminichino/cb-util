##
##

import argparse
import warnings
from overrides import override
from cbcmgr import VERSION
from cbcmgr.cli.cli import CLI
from cbcmgr.cli.exceptions import *
from cbcmgr.cb_capella import Capella, CapellaCluster, AllowedCIDR, Credentials
from cbcmgr.cb_bucket import Bucket
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
        opt_parser.add_argument('-d', '--db', action='store', help="Database for bucket operations")
        opt_parser.add_argument('-a', '--allow', action='store', help="Allow CIDR", default="0.0.0.0/0")
        opt_parser.add_argument('-c', '--cidr', action='store', help="Cluster CIDR", default="10.0.0.0/23")
        opt_parser.add_argument('-m', '--machine', action='store', help="Machine type", default="4x16")
        opt_parser.add_argument('-U', '--user', action='store', help="User Name", default="Administrator")
        opt_parser.add_argument('-P', '--password', action='store', help="User Password")
        opt_parser.add_argument('-C', '--cloud', action='store', help="Cluster cloud", default="aws")
        opt_parser.add_argument('-R', '--region', action='store', help="Cloud region", default="us-east-1")
        opt_parser.add_argument('-r', '--replicas', action='store', help="Bucket replicas", default=1, type=int)
        opt_parser.add_argument('-q', '--quota', action='store', help="Bucket quota", default=128, type=int)
        opt_parser.add_argument('-t', '--ttl', action='store', help="Bucket TTL", default=0, type=int)

        command_subparser = self.parser.add_subparsers(dest='command')
        cluster_parser = command_subparser.add_parser('cluster', help="Cluster Operations", parents=[opt_parser], add_help=False)
        cluster_subparser = cluster_parser.add_subparsers(dest='cluster_command')
        cluster_subparser.add_parser('get', help="Get cluster info", parents=[opt_parser], add_help=False)
        cluster_subparser.add_parser('list', help="List clusters", parents=[opt_parser], add_help=False)
        cluster_subparser.add_parser('create', help="Create clusters", parents=[opt_parser], add_help=False)
        cluster_subparser.add_parser('delete', help="Delete clusters", parents=[opt_parser], add_help=False)
        project_parser = command_subparser.add_parser('project', help="Cluster Operations", parents=[opt_parser], add_help=False)
        project_subparser = project_parser.add_subparsers(dest='project_command')
        project_subparser.add_parser('get', help="Get project info", parents=[opt_parser], add_help=False)
        project_subparser.add_parser('list', help="List projects", parents=[opt_parser], add_help=False)
        org_parser = command_subparser.add_parser('org', help="Cluster Operations", parents=[opt_parser], add_help=False)
        org_subparser = org_parser.add_subparsers(dest='org_command')
        org_subparser.add_parser('get', help="Get organization info", parents=[opt_parser], add_help=False)
        org_subparser.add_parser('list', help="List organizations", parents=[opt_parser], add_help=False)
        bucket_parser = command_subparser.add_parser('bucket', help="Bucket Operations", parents=[opt_parser], add_help=False)
        bucket_subparser = bucket_parser.add_subparsers(dest='bucket_command')
        bucket_subparser.add_parser('create', help="Create bucket", parents=[opt_parser], add_help=False)
        bucket_subparser.add_parser('delete', help="Delete bucket", parents=[opt_parser], add_help=False)
        bucket_subparser.add_parser('list', help="List buckets", parents=[opt_parser], add_help=False)

    def create_cluster(self, project_id: str):
        cluster_name = self.options.name
        cluster_cloud = self.options.cloud
        cluster_region = self.options.region
        cluster_cidr = self.options.cidr
        cluster_machine = self.options.machine
        allow_cidr = self.options.allow
        username = self.options.user
        if self.options.password:
            password = self.options.password
        else:
            password = Capella().generate_password()
            logger.info(f"Password: {password}")

        cluster = CapellaCluster().create(cluster_name, "CapUtil generated cluster", cluster_cloud, cluster_region, cluster_cidr)
        cluster.add_service_group(cluster_cloud, cluster_machine)

        logger.info("Creating cluster")
        cluster_id = Capella(project_id=project_id).create_cluster(cluster)

        logger.info("Waiting for cluster creation to complete")
        Capella(project_id=project_id).wait_for_cluster(cluster_name)

        logger.info(f"Cluster ID: {cluster_id}")

        cidr = AllowedCIDR().create(allow_cidr)

        logger.info(f"Configuring allowed CIDR {allow_cidr}")
        Capella(project_id=project_id).allow_cidr(cluster_id, cidr)

        credentials = Credentials().create(username, password)

        logger.info(f"Creating database user {username}")
        Capella(project_id=project_id).add_db_user(cluster_id, credentials)
        logger.info("Done")

    def delete_cluster(self, project_id: str):
        cluster_name = self.options.name

        logger.info(f"Destroying cluster {cluster_name}")
        Capella(project_id=project_id).delete_cluster(cluster_name)
        logger.info("Waiting for cluster deletion to complete")
        Capella(project_id=project_id).wait_for_cluster_delete(cluster_name)

    def create_bucket(self, project_id: str):
        database = self.options.db
        bucket_name = self.options.name
        bucket_quota = self.options.quota
        bucket_replicas = self.options.replicas
        bucket_ttl = self.options.ttl
        bucket = Bucket(**dict(
            name=bucket_name,
            ram_quota_mb=bucket_quota,
            num_replicas=bucket_replicas,
            max_ttl=bucket_ttl
        ))

        cluster = Capella(project_id=project_id).get_cluster(database)
        if cluster:
            cluster_id = cluster.get('id')
            logger.info(f"Creating bucket {bucket_name}")
            Capella(project_id=project_id).add_bucket(cluster_id, bucket)

    def delete_bucket(self, project_id: str):
        database = self.options.db
        bucket_name = self.options.name
        logger.info(f"Deleting bucket {bucket_name}")
        Capella(project_id=project_id).delete_bucket(database, bucket_name)

    def run(self):
        logger.info("CapUtil version %s" % VERSION)
        cm = Capella()
        project = cm.get_project(self.options.project)
        project_id = None
        if project:
            project_id = project.get('id')

        if self.options.command == 'cluster':
            if not project_id:
                logger.error(f"Can not find project {self.options.project}")
                return

            if self.options.cluster_command == "create":
                self.create_cluster(project_id)
                return
            elif self.options.cluster_command == "delete":
                self.delete_cluster(project_id)
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
        elif self.options.command == 'bucket':
            if not project_id:
                logger.error(f"Can not find project {self.options.project}")
                return

            if self.options.bucket_command == "create":
                self.create_bucket(project_id)
                return
            elif self.options.bucket_command == "delete":
                self.delete_bucket(project_id)
                return

            cluster = Capella(project_id=project_id).get_cluster(self.options.db)
            if cluster:
                cluster_id = cluster.get('id')
                data = cm.list_buckets(cluster_id)
                df = pd.json_normalize(data)
                subset_df = df

                if self.options.bucket_command == "get":
                    result = pd.DataFrame(subset_df[(subset_df.name == self.options.name)])
                    if not result.empty:
                        print(result)
                elif self.options.bucket_command == "list":
                    print(pd.DataFrame(subset_df).to_string())
        elif self.options.command == 'project':
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
