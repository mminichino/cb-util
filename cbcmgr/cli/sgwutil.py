##
##

import json
import sys
import argparse
import warnings
import logging
import re
from overrides import override
from typing import Tuple, List
from cbcmgr import VERSION
from cbcmgr.cli.cli import CLI
from cbcmgr.cb_connect import CBConnect
from cbcmgr.cb_management import CBManager
from cbcmgr.httpsessionmgr import APISession
from cbcmgr.exceptions import HTTPForbidden, HTTPNotImplemented, PreconditionFailed, ConflictException
from cbcmgr.retry import retry
from cbcmgr.schema import ProcessSchema, Schema

warnings.filterwarnings("ignore")
logger = logging.getLogger()
ignore_errors = False


class CBSInterface(object):

    def __init__(self, hostname, username, password, ssl=True):
        self.host = hostname
        self.username = username
        self.password = password
        self.ssl = ssl

    def import_schema(self, bucket: str) -> Schema:
        dbm = CBManager(self.host, self.username, self.password, ssl=self.ssl)
        contents = dbm.cluster_schema_dump()
        inventory = ProcessSchema(json_data=contents).inventory()
        return inventory.get(bucket)

    def merge(self, src: dict, dst: dict):
        for key in src:
            if key in dst:
                if isinstance(src[key], dict) and isinstance(dst[key], dict):
                    dst[key] = self.merge(src[key], dst[key])
                    continue
            dst[key] = src[key]
        return dst

    def keyspace_list(self, keyspace: str) -> Tuple[List[str], dict]:
        collection_list = []
        scope_struct = {}
        elements = keyspace.split('.')
        if len(elements) < 3:
            elements.append(r".*")
        if len(elements) < 3:
            elements.append(r".*")

        schema = self.import_schema(elements[0])

        for bucket in schema.buckets:
            for scope in bucket.scopes:
                if not re.match(f"^{elements[1]}$", scope.name) or scope.name == '_default':
                    continue
                for collection in scope.collections:
                    if not re.match(f"^{elements[2]}$", collection.name) or collection.name == '_default':
                        continue
                    keyspace_string = '.'.join([bucket.name, scope.name, collection.name])
                    logger.debug(f"Adding keyspace {keyspace_string}")
                    collection_list.append(keyspace_string)
                    add_struct = {
                        "scopes": {
                            scope.name: {
                                "collections": {
                                    collection.name: {}
                                }
                            }
                        }
                    }
                    scope_struct = self.merge(add_struct, scope_struct)

        if len(collection_list) == 0:
            collection_list.append(elements[0])

        return collection_list, scope_struct

    def get_users_by_field(self, field, keyspace):
        usernames = []
        collection_list, _ = self.keyspace_list(keyspace)
        db = CBConnect(self.host, self.username, self.password, ssl=self.ssl).connect()

        for collection in collection_list:
            query = f"select distinct {field} from {collection} where {field} is not missing;"
            logger.debug(f"get_users_by_field query: {query}")
            try:
                results = db.cb_query(sql=query)
                if not results:
                    continue
                for record in results:
                    value = record[field]
                    usernames.append(f"{field}@{value}")
            except Exception as err:
                logger.error(f"Can not get the values for {field}: {err}")
                sys.exit(1)

        return list(set(usernames))


class SGWDatabase(APISession):

    def __init__(self, node, *args, port=4985, ssl=0, **kwargs):
        super().__init__(*args, **kwargs)
        self.hostname = node
        self.set_host(node, ssl, port)

    def create(self, bucket, name, replicas: int = 0, keyspace_struct: dict = None):
        data = {
            "import_docs": True,
            "enable_shared_bucket_access": True,
            "bucket": bucket,
            "name": name,
            "num_index_replicas": replicas
        }

        if keyspace_struct:
            data.update(keyspace_struct)

        logger.debug(f"Database create POST data: {json.dumps(data)}")

        try:
            self.api_put(f"/{name}/", data)
            logger.info(f"Database {name} created for bucket {bucket}.")
        except HTTPForbidden:
            logger.error(f"Bucket {bucket} does not exist.")
            sys.exit(1)
        except PreconditionFailed:
            logger.error(f"Database {name} already exists.")
            if not ignore_errors:
                sys.exit(1)
        except Exception as err:
            logger.error(f"Database create failed for bucket {bucket}: {err}")
            sys.exit(1)

    def delete(self, name):
        try:
            self.api_delete(f"/{name}/")
            logger.info(f"Database {name} deleted.")
        except HTTPForbidden:
            logger.error(f"Database {name} does not exist.")
            sys.exit(1)
        except Exception as err:
            logger.error(f"Database delete failed for {name}: {err}")
            sys.exit(1)

    def expand_name(self, name) -> List[str]:
        if len(name.split('.')) != 1:
            return [name]

        response = self.api_get(f"/{name}/_config").json()
        if 'scopes' in response:
            keyspace_list = []
            for key in response['scopes']:
                prefix = f"{name}.{key}."
                for collection in response['scopes'][key].get('collections', {}).keys():
                    keyspace_list.append(f"{prefix}{collection}")
            return keyspace_list
        else:
            return [name]

    def sync_fun(self, name, filename):
        keyspace_list = self.expand_name(name)

        with open(filename, "r") as file:
            data = file.read()
            file.close()
            for keyspace in keyspace_list:
                try:
                    self.api_put_data(f"/{keyspace}/_config/sync", data, 'application/javascript')
                    logger.info(f"Sync function created for database {keyspace}.")
                except HTTPForbidden:
                    logger.error(f"Database {keyspace} does not exist.")
                    sys.exit(1)
                except Exception as err:
                    logger.error(f"Sync function create failed for database {keyspace}: {err}")
                    sys.exit(1)

    def get_sync_fun(self, name):
        try:
            response = self.api_get(f"/{name}/_config/sync")
            logger.info(response.response)
        except HTTPForbidden:
            logger.error(f"Database {name} does not exist.")
            sys.exit(1)
        except Exception as err:
            logger.error(f"Sync function get failed for database {name}: {err}")
            sys.exit(1)

    def resync(self, name):
        try:
            self.api_post(f"/{name}/_offline", None)
            self.api_post(f"/{name}/_resync", None)
            logger.info("Waiting for resync to complete")
            self.resync_wait(name)
            logger.info("Resync complete")
        except HTTPForbidden:
            logger.error(f"Database {name} does not exist.")
            sys.exit(1)
        except Exception as err:
            logger.error(f"Resync failed for database {name}: {err}")
            sys.exit(1)

    @retry(retry_count=12)
    def resync_wait(self, name):
        self.api_post(f"/{name}/_online", None)

    def list(self, name):
        try:
            response = self.api_get(f"/{name}/_config").json()
            logger.info(f"Bucket:   {response['bucket']}")
            logger.info(f"Name:     {response['name']}")
            logger.info(f"Replicas: {response['num_index_replicas']}")
            if 'scopes' in response:
                logger.info("Scopes:")
                logger.info(json.dumps(response['scopes'], indent=2))
        except HTTPForbidden:
            logger.error(f"Database {name} does not exist.")
            sys.exit(1)
        except Exception as err:
            logger.error(f"Database list failed for {name}: {err}")
            sys.exit(1)

    def list_all(self):
        try:
            response = self.api_get("/_all_dbs").json()
            for database in response:
                logger.info(database)
        except Exception as err:
            logger.error(f"Database list failed: {err}")
            sys.exit(1)

    @retry(retry_count=12)
    def ready_wait(self, name):
        self.api_get(f"/{name}/_config").json()

    def dump(self, name):
        keyspace_list = self.expand_name(name)

        for keyspace in keyspace_list:
            logger.info(f"Keyspace {keyspace}:")
            try:
                response = self.api_get(f"/{keyspace}/_all_docs").json()
                for item in response["rows"]:
                    document = self.api_get(f"/{keyspace}/_raw/{item['id']}").json()
                    sequence = document['_sync']['sequence']
                    offset = document['_sync']['recent_sequences'].index(sequence)
                    logger.info(f"Key: {item['key']} "
                                f"Id: {item['id']} "
                                f"Channels: {document['_sync']['history']['channels'][offset]}")
            except HTTPForbidden:
                logger.error(f"Database {keyspace} does not exist.")
                sys.exit(1)
            except Exception as err:
                logger.error(f"Database list failed for {keyspace}: {err}")
                sys.exit(1)


class SGWUser(APISession):

    def __init__(self, node, *args, port=4985, ssl=0, **kwargs):
        super().__init__(*args, **kwargs)
        self.hostname = node
        self.set_host(node, ssl, port)

    def create(self, dbname, username, password, channels=None):
        if channels is None:
            admin_channels = "*"
        else:
            admin_channels = channels
        data = {
            "password": password,
            "admin_channels": [admin_channels],
            "disabled": False
        }
        try:
            self.api_put(f"/{dbname}/_user/{username}", data)
            logger.info(f"User {username} created for database {dbname}.")
        except HTTPForbidden:
            logger.error(f"Database {dbname} does not exist.")
            sys.exit(1)
        except ConflictException:
            logger.error(f"User {username} already exists.")
            if not ignore_errors:
                sys.exit(1)
        except Exception as err:
            logger.error(f"User create failed for database {dbname}: {err}")
            sys.exit(1)

    def delete(self, name, username):
        try:
            self.api_delete(f"/{name}/_user/{username}")
            logger.info(f"User {username} deleted from {name}.")
        except HTTPForbidden:
            logger.error(f"Database {name} does not exist.")
            sys.exit(1)
        except HTTPNotImplemented:
            logger.error(f"User {username} does not exist.")
            sys.exit(1)
        except Exception as err:
            logger.error(f"Database delete failed for {name}: {err}")
            sys.exit(1)

    def list(self, name, username=None):
        try:
            if username:
                response = self.api_get(f"/{name}/_user/{username}").json()
                logger.info(f"Name:           {response['name']}")
                logger.info(f"Admin channels: {response['admin_channels']}")
                logger.info(f"All channels:   {response.get('all_channels', 'None')}")
                logger.info(f"Roles:          {response.get('admin_roles', 'None')}")
                logger.info(f"Disabled:       {response.get('disabled', 'None')}")
            else:
                response = self.api_get(f"/{name}/_user/").json()
                for item in response:
                    logger.info(item)
        except HTTPForbidden:
            logger.error(f"Database {name} does not exist.")
            sys.exit(1)
        except HTTPNotImplemented:
            logger.error(f"User {username} does not exist.")
            sys.exit(1)
        except Exception as err:
            logger.error(f"Database list failed for {name}: {err}")
            sys.exit(1)


class SGWAuth(APISession):

    def __init__(self, node, *args, port=4985, ssl=0, **kwargs):
        super().__init__(*args, **kwargs)
        self.hostname = node
        self.set_host(node, ssl, port)

    def get_session(self, name, user):
        data = {
            "name": user
        }
        response = self.api_post(f"/{name}/_session", data)
        logger.info(json.dumps(json.loads(response.response), indent=2))


class SGWServer(APISession):

    def __init__(self, node, *args, port=4985, ssl=0, **kwargs):
        super().__init__(*args, **kwargs)
        self.hostname = node
        self.set_host(node, ssl, port)

    def get_info(self):
        response = self.api_get("/").json()
        return response

    def print_info(self):
        info = self.get_info()
        if info.get('version'):
            name, version = info.get('version').split('/')[0:2]
            version = version.split('(', 1)[0]
            logger.info(f"{name} {version}")
        else:
            logger.error("Can not get server information")


class RunMain(CLI):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @override()
    def local_args(self):
        opt_parser = argparse.ArgumentParser(parents=[self.parser], add_help=False)
        opt_parser.add_argument('-u', '--user', action='store', help="User Name", default="Administrator")
        opt_parser.add_argument('-p', '--password', action='store', help="User Password", default="password")
        opt_parser.add_argument('-h', '--host', action='store', help="Sync Gateway Hostname", default="localhost")
        opt_parser.add_argument('-s', '--ssl', action='store_true', help="Use SSL")
        opt_parser.add_argument('-k', '--keyspace', action='store', help='Keyspace')
        opt_parser.add_argument('-b', '--bucket', action='store', help='Bucket name')
        opt_parser.add_argument('-d', '--dbhost', action='store', help='Couchbase hostname', default="localhost")
        opt_parser.add_argument('-l', '--dblogin', action='store', help='Couchbase credentials', default="Administrator:password")
        opt_parser.add_argument('--dbuser', action='store', help='Couchbase user', default="Administrator")
        opt_parser.add_argument('--dbpass', action='store', help='Couchbase password', default="password")
        opt_parser.add_argument('--help', action='help', default=argparse.SUPPRESS, help='Show help message')
        opt_parser.add_argument('-i', '--ignore', action='store_true', help="Ignore errors")
        opt_parser.add_argument('-n', '--name', action='store', help='Database name')
        opt_parser.add_argument('-f', '--function', action='store', help='Sync Function')
        opt_parser.add_argument('-r', '--replicas', action='store', help='Replica count', type=int, default=0)
        opt_parser.add_argument('-g', '--get', action='store_true', help='Get Sync Function')
        opt_parser.add_argument('-U', '--sguser', action='store', help='SGW user name', default="sguser")
        opt_parser.add_argument('-P', '--sgpass', action='store', help='SGW user password', default="password")
        opt_parser.add_argument('-F', '--field', action='store', help='Document field')
        opt_parser.add_argument('-a', '--all', action='store_true', help='List all users')

        command_subparser = self.parser.add_subparsers(dest='command')
        command_subparser.add_parser('version', help="Show versions", parents=[opt_parser], add_help=False)
        db_parser = command_subparser.add_parser('database', help="Database Operations", parents=[opt_parser], add_help=False)
        db_subparser = db_parser.add_subparsers(dest='db_command')
        db_subparser.add_parser('create', help="Create Database", parents=[opt_parser], add_help=False)
        db_subparser.add_parser('delete', help="Delete Database", parents=[opt_parser], add_help=False)
        db_subparser.add_parser('sync', help="Add Sync Function", parents=[opt_parser], add_help=False)
        db_subparser.add_parser('resync', help="Sync Documents", parents=[opt_parser], add_help=False)
        db_subparser.add_parser('list', help="List Databases", parents=[opt_parser], add_help=False)
        db_subparser.add_parser('dump', help="Dump Databases", parents=[opt_parser], add_help=False)
        db_subparser.add_parser('wait', help="Wait For Database Online", parents=[opt_parser], add_help=False)
        user_parser = command_subparser.add_parser('user', help="User Operations", parents=[opt_parser], add_help=False)
        user_subparser = user_parser.add_subparsers(dest='user_command')
        user_subparser.add_parser('create', help="Add User", parents=[opt_parser], add_help=False)
        user_subparser.add_parser('delete', help="Delete User", parents=[opt_parser], add_help=False)
        user_subparser.add_parser('list', help="List Users", parents=[opt_parser], add_help=False)
        user_subparser.add_parser('map', help="Map values to users", parents=[opt_parser], add_help=False)
        auth_parser = command_subparser.add_parser('auth', help="User Operations", parents=[opt_parser], add_help=False)
        auth_subparser = auth_parser.add_subparsers(dest='auth_command')
        auth_subparser.add_parser('session', help="Get Session", parents=[opt_parser], add_help=False)
        server_parser = command_subparser.add_parser('server', help="Server Operations", parents=[opt_parser], add_help=False)
        server_subparser = server_parser.add_subparsers(dest='server_command')
        server_subparser.add_parser('info', help="Get Server Info", parents=[opt_parser], add_help=False)

    def run(self):
        global ignore_errors
        logger.info(f"Sync Gateway CLI ({VERSION})")
        keyspace_struct = None
        hostname = self.options.host
        username = self.options.user
        password = self.options.password
        ssl = self.options.ssl
        db_name = self.options.name
        bucket = self.options.bucket
        keyspace = self.options.keyspace
        cbs_host = self.options.dbhost
        cbs_username = self.options.dbuser
        cbs_password = self.options.dbpass
        replicas = self.options.replicas
        sync_function = self.options.function
        sgw_username = self.options.sguser
        sgw_password = self.options.sgpass
        field = self.options.field

        if self.options.ignore:
            ignore_errors = True

        if self.options.command == 'version':
            sys.exit(0)

        if self.options.debug:
            logger.setLevel(logging.DEBUG)

        if self.options.command == 'database':
            sgdb = SGWDatabase(hostname, username, password, ssl=ssl)

            if self.options.db_command == "create":
                if not db_name:
                    db_name = bucket
                if keyspace:
                    cbdb = CBSInterface(cbs_host, cbs_username, cbs_password)
                    _, keyspace_struct = cbdb.keyspace_list(keyspace)
                sgdb.create(bucket, db_name, replicas, keyspace_struct)

            elif self.options.db_command == "delete":
                sgdb.delete(db_name)

            elif self.options.db_command == "sync":
                if self.options.get:
                    sgdb.get_sync_fun(db_name)
                else:
                    sgdb.sync_fun(db_name, sync_function)
                    sgdb.resync(db_name)

            elif self.options.db_command == 'resync':
                sgdb.resync(db_name)

            elif self.options.db_command == "list":
                if db_name:
                    sgdb.list(db_name)
                else:
                    sgdb.list_all()

            elif self.options.db_command == "dump":
                sgdb.dump(db_name)

            elif self.options.db_command == "wait":
                sgdb.ready_wait(db_name)

        elif self.options.command == 'user':
            sguser = SGWUser(hostname, username, password, ssl=ssl)

            if self.options.user_command == "create":
                sguser.create(db_name, sgw_username, sgw_password)

            elif self.options.user_command == "delete":
                sguser.delete(db_name, sgw_username)

            elif self.options.user_command == "list":
                if self.options.all:
                    sguser.list(db_name)
                else:
                    sguser.list(db_name, sgw_username)

            elif self.options.user_command == "map":
                if not field:
                    logger.error(f"User map requires document field parameter (-F)")
                    sys.exit(1)
                if not keyspace:
                    logger.error(f"User map requires query keyspace parameter (-k)")
                    sys.exit(1)
                cbdb = CBSInterface(cbs_host, cbs_username, cbs_password)
                username_list = cbdb.get_users_by_field(field, keyspace)
                for user in username_list:
                    sguser.create(db_name, user, sgw_password, channels=f"channel.{user}")

        elif self.options.command == 'auth':
            sgauth = SGWAuth(hostname, username, password, ssl=ssl)

            if self.options.auth_command == "session":
                sgauth.get_session(db_name, sgw_username)

        elif self.options.command == 'server':
            sgserver = SGWServer(hostname, username, password, ssl=ssl)

            if self.options.server_command == "info":
                sgserver.print_info()


def main(args=None):
    cli = RunMain(args)
    cli.run()
    sys.exit(0)
