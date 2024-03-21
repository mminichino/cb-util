##
##

import concurrent.futures
import logging
from cbcmgr.exceptions import TaskError
from cbcmgr.cb_session import BucketMode
from cbcmgr.cb_operation_s import CBOperation, Operation

logger = logging.getLogger('cbutil.mt.pool')
logger.addHandler(logging.NullHandler())


class CBPool(object):

    def __init__(self,
                 hostname: str,
                 username: str,
                 password: str,
                 ssl=False,
                 external=False,
                 kv_timeout: int = 5,
                 query_timeout: int = 60,
                 create: bool = False,
                 quota: int = 256,
                 replicas: int = 0,
                 mode: BucketMode = BucketMode.DEFAULT):
        self.keyspace = {}
        self.tasks = set()
        self.executor = concurrent.futures.ThreadPoolExecutor()
        self.hostname = hostname
        self.username = username
        self.password = password
        self.ssl = ssl
        self.external = external
        self.kv_timeout = kv_timeout
        self.query_timeout = query_timeout
        self.create = create
        self.quota = quota
        self.replicas = replicas
        self.mode = mode

    def connect(self, keyspace):
        if keyspace in self.keyspace:
            return
        logger.debug(f"pool add: cluster {self.hostname} keyspace {keyspace}")
        self.keyspace[keyspace] = CBOperation(self.hostname,
                                              self.username,
                                              self.password,
                                              ssl=self.ssl,
                                              kv_timeout=self.kv_timeout,
                                              query_timeout=self.query_timeout,
                                              quota=self.quota,
                                              replicas=self.replicas,
                                              mode=self.mode,
                                              create=self.create).connect(keyspace)

    def dispatch(self, keyspace: str, op: Operation, *args):
        opm = self.keyspace[keyspace]
        operator = opm.get_operator(op)
        operator.prep(*args)
        self.tasks.add(self.executor.submit(operator.execute))

    def join(self):
        while self.tasks:
            done, self.tasks = concurrent.futures.wait(self.tasks, return_when=concurrent.futures.FIRST_COMPLETED)
            for task in done:
                try:
                    task.result()
                except Exception as err:
                    raise TaskError(err)

    def shutdown(self):
        self.executor.shutdown()
        for opm in self.keyspace.values():
            opm.close()
