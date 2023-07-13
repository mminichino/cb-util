##
##

import os
import logging
import asyncio
from cbcmgr.exceptions import TaskError
from cbcmgr.cb_session import BucketMode
from cbcmgr.cb_operation_a import CBOperationAsync, Operation

logger = logging.getLogger('cbutil.async.pool')
logger.addHandler(logging.NullHandler())


class CBPoolAsync(object):

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
        self.loop = asyncio.get_event_loop()
        self.hostname = hostname
        self.username = username
        self.password = password
        self.ssl = ssl
        self.external = external
        self.create = create
        self.quota = quota
        self.replicas = replicas
        self.mode = mode
        self.max_tasks = min(100, os.cpu_count() * 10)
        self.factor = 0.01

    async def connect(self, keyspace):
        if keyspace in self.keyspace:
            return
        logger.debug(f"pool add: {self.hostname} {keyspace} create is {self.create}")
        opc = CBOperationAsync(self.hostname,
                               self.username,
                               self.password,
                               ssl=self.ssl,
                               quota=self.quota,
                               replicas=self.replicas,
                               mode=self.mode,
                               create=self.create)
        opm = await opc.init()
        self.keyspace[keyspace] = await opm.connect(keyspace)

    async def dispatch(self, keyspace: str, op: Operation, *args):
        retry_number = 0
        while len(asyncio.all_tasks()) > self.max_tasks:
            wait = self.factor
            wait *= (2 ** (retry_number + 1))
            await asyncio.sleep(wait)
        opm = self.keyspace[keyspace]
        operator = opm.get_operator(op)
        operator.prep(*args)
        self.tasks.add(self.loop.create_task(operator.execute()))

    async def join(self):
        if len(self.tasks) > 0:
            await asyncio.sleep(0)
            results = await asyncio.gather(*self.tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    raise TaskError(f"task error: {result}")
