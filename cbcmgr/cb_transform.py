##
##

import concurrent.futures
import logging
import json
import time
import multiprocessing
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
        self.start_time = time.perf_counter()
        self._error_count = multiprocessing.Value('i', 0)
        self._run_count: int = 0
        self._ops_per_sec: float = 1.0

    def process(self, source: dict, transform: Type[Transform]):
        try:
            key, document = transform().transform(source)
            self.put_doc(self.collection, key, document)
        except Exception as e:
            with self._error_count.get_lock():
                self._error_count.value += 1
            logger.error(f"Transform failed: {e}")
            logger.error(f"Source:\n{json.dumps(source, indent=2)}")

    def dispatch(self, source: dict, transform: Type[Transform]):
        self.tasks.add(self.executor.submit(self.process, source, transform))
        now_time = time.perf_counter()
        self._run_count += 1
        run_duration = now_time - self.start_time
        if run_duration > 0:
            self._ops_per_sec = self._run_count / run_duration

    def join(self):
        while self.tasks:
            done, self.tasks = concurrent.futures.wait(self.tasks, return_when=concurrent.futures.FIRST_COMPLETED)
            for task in done:
                try:
                    task.result()
                except Exception as err:
                    raise TaskError(err)

    @property
    def ops_per_sec(self) -> float:
        return self._ops_per_sec

    @property
    def error_count(self) -> int:
        return self._error_count.value

    @property
    def run_count(self) -> int:
        return self._run_count

    @property
    def run_time(self) -> float:
        now_time = time.perf_counter()
        return now_time - self.start_time

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.executor.shutdown()
