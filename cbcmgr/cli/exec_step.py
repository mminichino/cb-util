##
##

import logging
import time
import re
from jinja2 import Template
from cbcmgr.cb_connect import CBConnect
from cbcmgr.cb_management import CBManager
from cbcmgr.cb_bucket import Bucket
from cbcmgr.cb_index import CBQueryIndex


class DBRead(object):

    def __init__(self, db: CBConnect, add_key: bool = False, key_field: str = 'doc_id'):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.db = db
        self._add_key = add_key
        self._key_field = key_field
        self._result = None

    def execute(self, key: str):
        self._result = self.db.cb_get(key)
        self.add_key(key)

    @property
    def result(self):
        return self._result

    def add_key(self, key: str):
        if self._result and self._add_key:
            self._result[self._key_field] = key

    def fetch(self, key: str):
        self.execute(key)
        return self.result


class DBWrite(object):

    def __init__(self, db: CBConnect, id_field: str = "record_id"):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.id_field = id_field
        self.db = db
        self._result = None

    def execute(self, key: str, document: dict, no_squash: bool = False):
        if no_squash:
            if self.db.cb_doc_exists(key):
                return None
        try:
            number = re.split(':', key)[-1]
            id_value = int(number)
        except (ValueError, TypeError):
            id_value = key
        begin_time = time.time()
        document[self.id_field] = id_value
        self._result = self.db.cb_upsert(key, document)
        end_time = time.time()
        total_time = end_time - begin_time
        self.logger.debug(f"write complete in {total_time:.6f}")
        return self._result

    @property
    def result(self):
        return self._result


class DBQuery(object):

    def __init__(self, db: CBConnect, query: str, **kwargs):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.query = query
        self.query_params = kwargs
        self.db = db
        self._result = None

    def execute(self):
        if self.query_params:
            t = Template(self.query)
            self.query = t.render(**self.query_params)
        self._result = self.db.cb_query(sql=self.query)

    @property
    def keyspace(self):
        return self.db.keyspace

    @property
    def result(self):
        return self._result


class DBManagement(object):

    def __init__(self, db: CBManager):
        self.db = db
        self._result = None

    def create_bucket(self, bucket: Bucket):
        self._result = self.db.create_bucket(bucket)

    def create_scope(self, bucket: str, scope: str):
        self.db.bucket_wait(bucket)
        self.db.bucket(bucket)
        self._result = self.db.create_scope(scope)

    def create_collection(self, bucket: str, scope: str, collection: str, max_ttl: int):
        self.db.bucket_wait(bucket)
        self.db.scope_wait(bucket, scope)
        self.db.bucket(bucket)
        self.db.scope(scope)
        self._result = self.db.create_collection(collection, max_ttl)

    def create_index(self, index: CBQueryIndex):
        self._result = self.db.cb_index_create(index)

    @property
    def result(self):
        return self._result
