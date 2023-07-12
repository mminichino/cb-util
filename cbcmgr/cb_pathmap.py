##
##

import json
import xmltodict
import logging
from cbcmgr.mt_pool import CBPool
from cbcmgr.config import UpsertMapConfig, MapUpsertType
from cbcmgr.exceptions import PathMapUpsertError
from cbcmgr.cb_session import BucketMode
from cbcmgr.util import omit_path, copy_path
from cbcmgr.id_format import doc_id_format
from cbcmgr.cb_operation_s import Operation

logger = logging.getLogger('cbutil.pathmap')
logger.addHandler(logging.NullHandler())


class CBPathMap(object):

    def __init__(self,
                 config: UpsertMapConfig,
                 hostname: str,
                 username: str,
                 password: str,
                 bucket: str,
                 scope: str,
                 ssl=False,
                 quota: int = 256,
                 replicas: int = 0,
                 mode: BucketMode = BucketMode.DEFAULT):
        self.config = config
        self.hostname = hostname
        self.username = username
        self.password = password
        self.bucket = bucket
        self.scope = scope
        self.ssl = ssl
        self.quota = quota
        self.replicas = replicas
        self.mode = mode
        self.pool = CBPool(self.hostname,
                           self.username,
                           self.password,
                           ssl=self.ssl,
                           quota=self.quota,
                           replicas=self.replicas,
                           mode=self.mode,
                           create=True)

    def connect(self):
        for c in self.config.paths:
            keyspace = f"{self.bucket}.{self.scope}.{c.name}"
            self.pool.connect(keyspace)

    def load_data(self,
                  prefix: str,
                  json_file: str = None,
                  xml_file: str = None,
                  json_data: str = None,
                  xml_data: str = None):

        if json_file:
            with open(json_file, mode="r") as json_xml:
                data = json.load(json_xml)
        elif xml_file:
            with open(xml_file, mode="rb") as input_xml:
                contents = input_xml.read()
                data = xmltodict.parse(contents)
        elif json_data:
            data = json.loads(json_data)
        elif xml_data:
            data = xmltodict.parse(xml_data)
        else:
            raise PathMapUpsertError(f"JSON or XML input data is required")

        for c in self.config.paths:
            logger.debug(f"processing key {c.path} name {c.name}")

            subset = copy_path(c.path, data)

            if not subset or len(subset) == 0:
                if c.optional:
                    continue
                else:
                    raise PathMapUpsertError(f"path {c.path} not found in source data")

            keyspace = f"{self.bucket}.{self.scope}.{c.name}"
            self.pool.connect(keyspace)

            if c.exclude:
                logger.debug(f"excluding {','.join(c.exclude)}")
                subset = omit_path(subset, c.exclude)

            if c.p_type == MapUpsertType.DOCUMENT:
                doc_id = doc_id_format("%t", text=prefix)
                logger.debug(f"processing doc ID {doc_id}")
                document = {c.name: subset}
                self.pool.dispatch(keyspace, Operation.WRITE, doc_id, document)
            elif c.p_type == MapUpsertType.LIST:
                logger.debug(f"processing list")
                if not isinstance(subset, list):
                    raise PathMapUpsertError(f"path {c.path} type {type(subset)} incompatible with list mode")
                for n, document in enumerate(subset):
                    number = document.get(c.id_key, n)
                    doc_id = doc_id_format("%t%s%f%s%n", text=prefix, field=c.id_key, number=number)
                    self.pool.dispatch(keyspace, Operation.WRITE, doc_id, document)

        self.pool.join()
