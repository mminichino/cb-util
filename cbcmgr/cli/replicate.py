#
#

import re
import sys
import attr
import json
import logging
import time
import threading
import concurrent.futures
import cbcmgr.cli.config as config
from functools import partial
from queue import Queue, Empty
from enum import Enum
from typing import Optional
from datetime import timedelta
from cbcmgr.cb_bucket import Bucket
from cbcmgr.cb_index import CBQueryIndex
from couchbase.management.users import Role
from cbcmgr.cb_collection import Collection
from cbcmgr.cli.exceptions import ReplicationError
from cbcmgr.cb_operation_s import CBOperation

logger = logging.getLogger('cbutil.replicate')
logger.addHandler(logging.NullHandler())


@attr.s
class Output:
    BUCKETS: Optional[dict] = attr.ib(default={})
    INDEXES: Optional[dict] = attr.ib(default={})
    DATA: Optional[dict] = attr.ib(default={})


class EnumEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Enum):
            return obj.value
        elif isinstance(obj, Collection):
            # noinspection PyTypeChecker
            return attr.asdict(obj)
        elif isinstance(obj, timedelta):
            return int(obj.total_seconds())
        return json.JSONEncoder.default(self, obj)


class Replicator(object):

    def __init__(self, filters=None, deferred=True):
        self.output = {}
        if filters:
            self.filters = filters
        else:
            self.filters = []
        self.deferred = deferred
        self.bucket_filters = []
        self.scope_filters = []
        self.collection_filters = []
        self.user_filters = []
        self.group_filters = []
        self.q = Queue()

        self.process_filters()

    def process_filters(self):
        for f in self.filters:
            try:
                k = f.split(':')[0]
                r = f.split(':')[1]
            except IndexError:
                continue
            if k == 'bucket':
                self.bucket_filters.append(r)
            if k == 'scope':
                self.scope_filters.append(r)
            if k == 'collection':
                self.collection_filters.append(r)
            if k == 'user':
                self.user_filters.append(r)
            if k == 'group':
                self.group_filters.append(r)

    def source(self):
        writer = threading.Thread(target=self.stream_output_thread)
        writer.start()
        self.read_schema_from_db()
        self.end_stream()
        writer.join()

    def target(self):
        reader = threading.Thread(target=self.read_input_thread)
        reader.start()
        self.read_schema_from_input()
        reader.join()

    def stream_output_thread(self):
        while True:
            try:
                entry = self.q.get(block=False)
                data = json.loads(entry)
                if data.get('__CMD__') == 'STOP':
                    return
                print(entry)
            except Empty:
                time.sleep(0.1)
                continue

    def read_input_thread(self):
        decoder = json.JSONDecoder()
        content = sys.stdin
        buffer = ''
        for chunk in iter(partial(content.read, 131072), ''):
            buffer += chunk
            while buffer:
                try:
                    entry, position = decoder.raw_decode(buffer)
                    self.q.put(json.dumps(entry))
                    buffer = buffer[position:]
                    buffer = buffer.lstrip()
                except ValueError:
                    break
        self.end_stream()

    def end_stream(self):
        entry = {'__CMD__': 'STOP'}
        self.q.put(json.dumps(entry))

    def read_schema_from_db(self):
        operator = CBOperation(config.host, config.username, config.password, ssl=config.tls, project=config.capella_project, database=config.capella_db)

        bucket_list = operator.bucket_list
        index_list = operator.index_list
        user_list = operator.user_list
        group_list = operator.group_list

        for group in group_list:
            if any(re.search(rx, group.get('name')) for rx in self.group_filters):
                continue
            struct = {'__GROUP__': group}
            entry = json.dumps(struct, indent=2, cls=EnumEncoder)
            self.q.put(entry)

        for user in user_list:
            if any(re.search(rx, user.get('username')) for rx in self.user_filters):
                continue
            struct = {'__USER__': user}
            entry = json.dumps(struct, indent=2, cls=EnumEncoder)
            self.q.put(entry)

        for bucket in bucket_list:
            bucket_index_list = []
            scope_list = []
            bucket_struct = Bucket(**bucket)
            if any(re.search(rx, bucket_struct.name) for rx in self.bucket_filters):
                continue
            # noinspection PyTypeChecker
            payload = attr.asdict(bucket_struct)
            struct = {'__BUCKET__': payload}
            for scope in operator.scope_list(bucket_struct.name):
                if any(re.search(rx, scope.name) for rx in self.scope_filters):
                    continue
                scope_record = {scope.name: []}
                collection_list = operator.collection_list(bucket_struct.name, scope.name)
                for collection in collection_list:
                    if any(re.search(rx, collection.name) for rx in self.collection_filters):
                        continue
                    scope_record[scope.name].append(
                        Collection(
                            name=collection.name,
                            max_ttl=collection.max_ttl
                        )
                    )
                scope_list.append(scope_record)
            struct.update({'__SCOPE__': scope_list})
            for index in index_list:
                if index.keyspace_id == bucket_struct.name or index.bucket_id == bucket_struct.name:
                    bucket_index_list.append(attr.asdict(index))
            struct.update({'__INDEX__': bucket_index_list})
            entry = json.dumps(struct, indent=2, cls=EnumEncoder)
            self.q.put(entry)

    def read_schema_from_input(self):
        operator = CBOperation(config.host, config.username, config.password, create=True, ssl=config.tls, project=config.capella_project, database=config.capella_db)
        user_list = []
        group_list = []

        while True:
            try:
                entry = self.q.get(block=False)
                data = json.loads(entry)
                if data.get('__CMD__') == 'STOP':
                    break
                if data.get('__GROUP__'):
                    group_list.append(data.get('__GROUP__'))
                if data.get('__USER__'):
                    user_list.append(data.get('__USER__'))
                if data.get('__BUCKET__'):
                    bucket = Bucket.from_dict(data.get('__BUCKET__'))
                    if data.get('__SCOPE__'):
                        scope_list = data.get('__SCOPE__')
                        for scope_struct in scope_list:
                            for scope, collections in scope_struct.items():
                                for collection in collections:
                                    collection_name = collection.get('name')
                                    keyspace = f"{bucket.name}.{scope}.{collection_name}"
                                    logger.info(f"Replicating keyspace {keyspace}")
                                    operator.connect(keyspace, bucket)
                    if data.get('__INDEX__'):
                        index_list = data.get('__INDEX__')
                        for index in index_list:
                            entry = CBQueryIndex.from_dict(index)
                            logger.info(f"Replicating index [{entry.keyspace_id}] {entry.name}")
                            operator.index_create(entry, deferred=self.deferred)
            except Empty:
                time.sleep(0.1)
                continue

        for group in group_list:
            roles = []
            for role in group.get('roles', []):
                if role.get('scope') == '*':
                    role['scope'] = None
                if role.get('collection') == '*':
                    role['collection'] = None
                r = Role(**role)
                roles.append(r)
            logger.info(f"Creating group {group.get('name')}")
            operator.create_group(group.get('name'), group.get('description'), roles if len(roles) > 0 else None)

        for user in user_list:
            roles = []
            groups = []
            for role in user.get('roles', []):
                if role.get('scope') == '*':
                    role['scope'] = None
                if role.get('collection') == '*':
                    role['collection'] = None
                r = Role(**role)
                roles.append(r)
            if user.get('groups'):
                groups = user.get('groups')
            logger.info(f"Creating user {user.get('username')}")
            operator.create_user(user.get('username'), user.get('name'), user.get('password'), roles if len(roles) > 0 else None, groups if len(groups) > 0 else None)

    @staticmethod
    def task_wait(tasks):
        result_set = []
        while tasks:
            done, tasks = concurrent.futures.wait(tasks, return_when=concurrent.futures.FIRST_COMPLETED)
            for task in done:
                try:
                    result = task.result()
                    if result:
                        result_set.append(result)
                except Exception as err:
                    logger.error(f"task error: {type(err).__name__}: {err}")
                    raise ReplicationError(f"task failed: {err}")
        return result_set
