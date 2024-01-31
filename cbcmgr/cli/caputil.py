##
##

import json
import argparse
import warnings
from overrides import override
from cbcmgr import VERSION
from cbcmgr.cli.cli import CLI
from cbcmgr.cli.exceptions import *
from cbcmgr.cb_capella import Capella, CapellaCluster, AllowedCIDR, Credentials, CapellaClusterUpdate, SupportPlan, SupportTZ, AppService
from cbcmgr.cb_bucket import Bucket
from cbcmgr.util import ask_for_password
import pandas as pd

warnings.filterwarnings("ignore")
logger = logging.getLogger()


class CapellaCLI(CLI):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        aux_parser = argparse.ArgumentParser(argument_default=argparse.SUPPRESS)
        for arg in vars(self.options):
            aux_parser.add_argument('--' + arg)
        self.cli_args, _ = aux_parser.parse_known_args()

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
        opt_parser.add_argument('-e', '--email', action='store', help="User Email")
        opt_parser.add_argument('-P', '--password', action='store', help="User Password")
        opt_parser.add_argument('-C', '--cloud', action='store', help="Cluster cloud", default="aws")
        opt_parser.add_argument('-R', '--region', action='store', help="Cloud region", default="us-east-1")
        opt_parser.add_argument('-r', '--replicas', action='store', help="Bucket replicas", default=1, type=int)
        opt_parser.add_argument('-q', '--quota', action='store', help="Bucket quota", default=128, type=int)
        opt_parser.add_argument('-s', '--services', dest='services', action='store', help='Services', default='data,index,query')
        opt_parser.add_argument('-N', '--nodes', dest='nodes', action='store', help='Node Count', default=3, type=int)
        opt_parser.add_argument('-V', '--disk', dest='disk', action='store', help='Disk Size', default=256, type=int)
        opt_parser.add_argument('-t', '--ttl', action='store', help="Bucket TTL", default=0, type=int)

        command_subparser = self.parser.add_subparsers(dest='command')
        cluster_parser = command_subparser.add_parser('cluster', help="Cluster Operations", parents=[opt_parser], add_help=False)
        cluster_subparser = cluster_parser.add_subparsers(dest='cluster_command')
        cluster_subparser.add_parser('get', help="Get cluster info", parents=[opt_parser], add_help=False)
        cluster_subparser.add_parser('list', help="List clusters", parents=[opt_parser], add_help=False)
        cluster_subparser.add_parser('create', help="Create cluster", parents=[opt_parser], add_help=False)
        cluster_subparser.add_parser('update', help="Update cluster", parents=[opt_parser], add_help=False)
        cluster_subparser.add_parser('delete', help="Delete cluster", parents=[opt_parser], add_help=False)
        project_parser = command_subparser.add_parser('project', help="Cluster Operations", parents=[opt_parser], add_help=False)
        project_subparser = project_parser.add_subparsers(dest='project_command')
        project_subparser.add_parser('get', help="Get project info", parents=[opt_parser], add_help=False)
        project_subparser.add_parser('list', help="List projects", parents=[opt_parser], add_help=False)
        project_subparser.add_parser('owner', help="Set project owner", parents=[opt_parser], add_help=False)
        project_subparser.add_parser('create', help="Create project", parents=[opt_parser], add_help=False)
        project_subparser.add_parser('delete', help="Delete project", parents=[opt_parser], add_help=False)
        org_parser = command_subparser.add_parser('org', help="Cluster Operations", parents=[opt_parser], add_help=False)
        org_subparser = org_parser.add_subparsers(dest='org_command')
        org_subparser.add_parser('get', help="Get organization info", parents=[opt_parser], add_help=False)
        org_subparser.add_parser('list', help="List organizations", parents=[opt_parser], add_help=False)
        bucket_parser = command_subparser.add_parser('bucket', help="Bucket Operations", parents=[opt_parser], add_help=False)
        bucket_subparser = bucket_parser.add_subparsers(dest='bucket_command')
        bucket_subparser.add_parser('create', help="Create bucket", parents=[opt_parser], add_help=False)
        bucket_subparser.add_parser('delete', help="Delete bucket", parents=[opt_parser], add_help=False)
        bucket_subparser.add_parser('list', help="List buckets", parents=[opt_parser], add_help=False)
        credential_parser = command_subparser.add_parser('credential', help="DB Credential Operations", parents=[opt_parser], add_help=False)
        credential_subparser = credential_parser.add_subparsers(dest='credential_command')
        credential_subparser.add_parser('password', help="Change password", parents=[opt_parser], add_help=False)
        user_parser = command_subparser.add_parser('user', help="User Operations", parents=[opt_parser], add_help=False)
        user_subparser = user_parser.add_subparsers(dest='user_command')
        user_subparser.add_parser('get', help="Get user", parents=[opt_parser], add_help=False)
        user_subparser.add_parser('list', help="List users", parents=[opt_parser], add_help=False)
        app_svc_parser = command_subparser.add_parser('appservice', help="App Service Operations", parents=[opt_parser], add_help=False)
        app_svc_subparser = app_svc_parser.add_subparsers(dest='app_svc_command')
        app_svc_subparser.add_parser('create', help="Create App Service", parents=[opt_parser], add_help=False)
        app_svc_subparser.add_parser('delete', help="Delete App Service", parents=[opt_parser], add_help=False)
        app_svc_subparser.add_parser('list', help="List App Services", parents=[opt_parser], add_help=False)

    def create_cluster(self, project_id: str):
        cluster_name = self.options.name
        cluster_cloud = self.options.cloud
        cluster_region = self.options.region
        cluster_cidr = self.options.cidr
        cluster_machine = self.options.machine
        cluster_storage = self.options.disk
        cluster_size = self.options.nodes
        cluster_services = self.options.services.split(',')
        allow_cidr = self.options.allow
        username = self.options.user
        if self.options.password:
            password = self.options.password
        else:
            password = Capella().generate_password()
            logger.info(f"Password: {password}")

        cluster = CapellaCluster().create(cluster_name, "CapUtil generated cluster", cluster_cloud, cluster_region, cluster_cidr)
        cluster.add_service_group(cluster_cloud, cluster_machine, cluster_storage, cluster_size, cluster_services)

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

    def update_cluster(self, project_id: str):
        cluster_name = self.options.name
        cluster = Capella(project_id=project_id).get_cluster(cluster_name)
        cluster_name = cluster.get('name')
        cluster_description = cluster.get('description')
        cluster_plan = SupportPlan(cluster.get('support').get('plan'))
        cluster_tz = SupportTZ(cluster.get('support').get('timezone'))

        update = CapellaClusterUpdate().create(cluster_name, cluster_description, cluster_plan, cluster_tz)
        for sg in cluster.get('serviceGroups'):
            cloud = cluster.get('cloudProvider').get('type')
            if hasattr(self.cli_args, 'machine'):
                machine_type = self.cli_args.machine
            else:
                cpus = sg.get('node').get('compute').get('cpu')
                memory = sg.get('node').get('compute').get('ram')
                machine_type = f"{cpus}x{memory}"
            if hasattr(self.cli_args, 'disk'):
                storage = self.cli_args.disk
            else:
                storage = sg.get('node').get('disk').get('storage')
            if hasattr(self.cli_args, 'nodes'):
                quantity = self.cli_args.nodes
            else:
                quantity = sg.get('numOfNodes')
            if hasattr(self.cli_args, 'services'):
                services = sg.get('services')
                addition = self.cli_args.services.split(',')
                services.extend(addition)
            else:
                services = sg.get('services')
            update.add_service_group(cloud, machine_type, storage, quantity, services)

        logger.info("Updating cluster")
        Capella(project_id=project_id).update_cluster(update)

        logger.info("Waiting for cluster update to complete")
        Capella(project_id=project_id).wait_for_cluster(cluster_name)

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
        bucket = Bucket.from_dict(dict(
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

    def change_password(self, project_id: str):
        database = self.options.db
        user_name = self.options.name

        cluster = Capella(project_id=project_id).get_cluster(database)
        if cluster:
            cluster_id = cluster.get('id')
            logger.info(f"Changing password for user {user_name}")
            password = ask_for_password()
            if Capella().valid_password(password):
                Capella(project_id=project_id).change_db_user_password(cluster_id, user_name, password)
            else:
                logger.error("Password does not meet complexity requirements")

    def set_project_owner(self, project_id: str):
        email = self.options.email

        logger.info(f"Setting ownership of project")
        Capella().set_project_owner(project_id, email)

    def create_project(self):
        name = self.options.name
        email = self.options.email

        project = Capella().get_project(name)
        if project:
            logger.info(f"Project {name} already exists.")
            return

        Capella().create_project(name, email)

    def delete_project(self):
        name = self.options.name

        Capella().delete_project(name)

    def create_app_service(self, project_id: str):
        database = self.options.db
        app_svc_name = self.options.name
        app_svc_machine = self.options.machine
        app_svc_nodes = self.options.nodes

        cluster = Capella(project_id=project_id).get_cluster(database)
        if cluster:
            cluster_id = cluster.get('id')

            app_svc = AppService.create(app_svc_name, "CapUtil generated app service", app_svc_nodes, app_svc_machine, "3.0")

            logger.info("Creating app service")
            Capella(project_id=project_id).create_app_svc(cluster_id, app_svc)

            logger.info("Waiting for app service creation to complete")
            Capella(project_id=project_id).wait_for_app_svc(cluster_id)

            logger.info("Done")

    def delete_app_service(self, project_id: str):
        database = self.options.db

        cluster = Capella(project_id=project_id).get_cluster(database)
        if cluster:
            cluster_id = cluster.get('id')

            logger.info(f"Destroying app services for cluster {database}")
            Capella(project_id=project_id).delete_app_svc(cluster_id)
            logger.info("Waiting for app service deletion to complete")
            Capella(project_id=project_id).wait_for_app_svc_delete(cluster_id)

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
            elif self.options.cluster_command == "update":
                self.update_cluster(project_id)
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
            if self.options.project_command == "owner":
                self.set_project_owner(project_id)
                return
            if self.options.project_command == "create":
                self.create_project()
                return
            if self.options.project_command == "delete":
                self.delete_project()
                return

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
        elif self.options.command == 'credential':
            if not project_id:
                logger.error(f"Can not find project {self.options.project}")
                return

            if self.options.credential_command == "password":
                self.change_password(project_id)
        elif self.options.command == 'user':
            if self.options.user_command == "get":
                result = cm.get_user(self.options.email)
                if result:
                    print(json.dumps(result, indent=2))
            elif self.options.user_command == "list":
                data = cm.list_users()
                df = pd.json_normalize(data)
                subset_df = df[["id", "name", "email"]]
                print(pd.DataFrame(subset_df).to_string())
        elif self.options.command == 'appservice':
            if self.options.app_svc_command == "create":
                self.create_app_service(project_id)
                return
            if self.options.app_svc_command == "delete":
                self.delete_app_service(project_id)
                return

            pm = Capella(project_id=project_id)
            data = pm.list_app_svc()
            df = pd.json_normalize(data)
            subset_df = df[["id", "name", "nodes", "compute.cpu", "compute.ram"]]

            if self.options.app_svc_command == "list":
                print(pd.DataFrame(subset_df).to_string())


def main(args=None):
    cli = CapellaCLI(args)
    cli.run()
    sys.exit(0)
