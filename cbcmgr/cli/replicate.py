#
#

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
from cbcmgr.cb_management import CBManager
from cbcmgr.cli.exec_step import DBManagement
from cbcmgr.cb_bucket import Bucket
from cbcmgr.cb_index import CBQueryIndex
from cbcmgr.cb_collection import Collection
from cbcmgr.cli.exceptions import ReplicationError

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

    def __init__(self):
        self.output = {}
        self.q = Queue()

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
        dbm = CBManager(config.host, config.username, config.password, ssl=config.tls, project=config.capella_project, database=config.capella_db).connect()

        bucket_list = dbm.bucket_list_all()
        index_list = dbm.index_list_all()

        for bucket in bucket_list:
            bucket_index_list = []
            scope_list = []
            bucket_struct = Bucket(**bucket)
            # noinspection PyTypeChecker
            payload = attr.asdict(bucket_struct)
            struct = {'__BUCKET__': payload}
            dbm.bucket(bucket_struct.name)
            for scope in dbm.scope_list_all():
                scope_record = {scope.name: []}
                collection_list = dbm.collection_list_all(scope)
                for collection in collection_list:
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
        dbm = CBManager(config.host, config.username, config.password, ssl=config.tls, project=config.capella_project, database=config.capella_db).connect()

        while True:
            try:
                entry = self.q.get(block=False)
                data = json.loads(entry)
                if data.get('__CMD__') == 'STOP':
                    break
                if data.get('__BUCKET__'):
                    bucket = Bucket.from_dict(data.get('__BUCKET__'))
                    logger.info(f"Replicating bucket {bucket.name}")
                    dbm.create_bucket(bucket)
                    if data.get('__SCOPE__'):
                        scope_list = data.get('__SCOPE__')
                        for scope_struct in scope_list:
                            for scope, collections in scope_struct.items():
                                logger.info(f"Replicating scope {bucket.name}.{scope}")
                                dbm.create_scope(scope)
                                for collection in collections:
                                    logger.info(f"Replicating collection {bucket.name}.{scope}.{collection.get('name')}")
                                    collection_name = collection.get('name')
                                    max_ttl = collection.get('max_ttl')
                                    dbm.create_collection(collection_name, max_ttl)
                    if data.get('__INDEX__'):
                        index_list = data.get('__INDEX__')
                        for index in index_list:
                            entry = CBQueryIndex.from_dict(index)
                            logger.info(f"Replicating index [{entry.keyspace_id}] {entry.name}")
                            dbm.cb_index_create(entry)
            except Empty:
                time.sleep(0.1)
                continue

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
