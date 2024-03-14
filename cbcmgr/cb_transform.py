##
##

import concurrent.futures
import logging
import json
from typing import Type, Tuple
from cbcmgr.exceptions import TaskError
from cbcmgr.cb_operation_s import CBOperation

logger = logging.getLogger('cbutil.cb_transform')
logger.addHandler(logging.NullHandler())


class Transform:

    def __init__(self, *args, **kwargs):
        pass

    def transform(self, source: dict) -> Tuple[str, dict]:
        pass


class CBTransform(CBOperation):

    def __init__(self, *args, keyspace: str, **kwargs):
        super().__init__(*args, **kwargs)
        self.tasks = set()
        self.executor = concurrent.futures.ThreadPoolExecutor()
        self.connect(keyspace)

    def process(self, source: dict, transform: Type[Transform]):
        try:
            key, document = transform().transform(source)
            self.put_doc(self.collection, key, document)
        except Exception as e:
            logger.error(f"Transform failed: {e}")
            logger.error(f"Source:\n{json.dumps(source, indent=2)}")

    def dispatch(self, source: dict, transform: Type[Transform]):
        self.tasks.add(self.executor.submit(self.process, source, transform))

    def join(self):
        while self.tasks:
            done, self.tasks = concurrent.futures.wait(self.tasks, return_when=concurrent.futures.FIRST_COMPLETED)
            for task in done:
                try:
                    task.result()
                except Exception as err:
                    raise TaskError(err)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.executor.shutdown()
