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

        for bucket in bucket_list:
            bucket_struct = Bucket(**bucket)
            # noinspection PyTypeChecker
            payload = attr.asdict(bucket_struct)
            struct = {'__BUCKET__': payload}
            entry = json.dumps(struct, indent=2, cls=EnumEncoder)
            self.q.put(entry)

    def read_schema_from_input(self):
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=config.batch_size)
        dbm = CBManager(config.host, config.username, config.password, ssl=config.tls, project=config.capella_project, database=config.capella_db).connect()
        tasks = set()

        while True:
            try:
                entry = self.q.get(block=False)
                data = json.loads(entry)
                if data.get('__CMD__') == 'STOP':
                    break
                if data.get('__BUCKET__'):
                    bucket = Bucket(**data.get('__BUCKET__'))
                    db_op = DBManagement(dbm)
                    tasks.add(executor.submit(db_op.create_bucket, bucket))
            except Empty:
                time.sleep(0.1)
                continue

        self.task_wait(tasks)

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
