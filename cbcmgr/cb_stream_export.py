##
##

import concurrent.futures
import logging
import time
import zlib
import json
import collections
import threading
import multiprocessing
from functools import partial
from itertools import islice
from cbcmgr.cb_operation_s import CBOperation
from cbcmgr.util import progress_count

logger = logging.getLogger('cbutil.export.stream')
logger.addHandler(logging.NullHandler())


class StreamExport(CBOperation):

    def __init__(self, *args, keyspace: str, file_name: str, **kwargs):
        super().__init__(*args, **kwargs)
        self.tasks = set()
        self.executor = concurrent.futures.ThreadPoolExecutor()
        self.connect(keyspace)
        self.queue = collections.deque()
        self.file_name = file_name
        self.compressor = zlib.compressobj(9, zlib.DEFLATED, zlib.MAX_WBITS | 16)
        self.decompressor = zlib.decompressobj(zlib.MAX_WBITS | 16)
        self.terminate = threading.Event()
        self.batch_size = 50
        self._error_count = multiprocessing.Value('i', 0)
        self._ops_per_sec: float = 1.0
        self._run_count: int = 0
        self.start_time = time.perf_counter()

    def get_worker(self, doc_id: str):
        try:
            document = self.get(doc_id)
            data = dict(
                doc_id=doc_id,
                document=document
            )
            return data
        except Exception as err:
            with self._error_count.get_lock():
                self._error_count.value += 1
            logger.error(f"Get failed: {doc_id}: {err}")

    def put_worker(self, doc_id: str, data: dict):
        try:
            self.put(doc_id, data)
        except Exception as err:
            with self._error_count.get_lock():
                self._error_count.value += 1
            logger.error(f"Put failed: {doc_id}: {err}")

    def calc_ops_per_sec(self, n: int):
        now_time = time.perf_counter()
        self._run_count += n
        run_duration = now_time - self.start_time
        if run_duration > 0:
            self._ops_per_sec = self._run_count / run_duration

    def to_file(self):
        buffer = bytearray()
        with open(self.file_name, 'wb') as zip_file:
            while not self.terminate.is_set() or self.queue:
                try:
                    record = self.queue.pop()
                    buffer.extend(record)
                except IndexError:
                    time.sleep(0.5)
                else:
                    if len(buffer) >= 131072:
                        chunk = buffer[:131072]
                        zip_file.write(self.compressor.compress(chunk))
                        buffer = buffer[131072:]
            if len(buffer) > 0:
                zip_file.write(self.compressor.compress(buffer))
            zip_file.write(self.compressor.flush())

    def from_file(self):
        with open(self.file_name, 'rb') as zip_file:
            for chunk in iter(partial(zip_file.read, 131072), ''):
                if len(chunk) == 0:
                    break
                part = self.decompressor.decompress(chunk)
                self.queue.appendleft(part)
        self.terminate.set()

    def read_from_collection(self):
        total = 0
        for doc_list in self.slice(self.doc_list(), self.batch_size):
            self.tasks.clear()
            for doc_id in doc_list:
                self.tasks.add(self.executor.submit(self.get_worker, doc_id))
            results = self.task_wait(self.tasks)
            for data in results:
                block = json.dumps(data).encode('utf-8')
                self.queue.appendleft(block + b'\n')
                total += 1
            self.calc_ops_per_sec(self.batch_size)
            progress_count(total, errors=self.error_count, ops_per_sec=self.ops_per_sec)
        self.terminate.set()
        progress_count(total, finished=True, errors=self.error_count, ops_per_sec=self.ops_per_sec)

    def write_to_collection(self):
        decoder = json.JSONDecoder()
        buffer = ''
        total = 0
        while not self.terminate.is_set() or self.queue:
            try:
                record = self.queue.pop()
                contents = record.decode('utf-8')
                buffer += contents
                self.tasks.clear()
                count = 0
                while buffer:
                    try:
                        json_object, position = decoder.raw_decode(buffer)
                        data = dict(json_object).copy()
                        doc_id = data.get('doc_id')
                        document = data.get('document', {})
                        if not doc_id or not document:
                            with self._error_count.get_lock():
                                self._error_count.value += 1
                            logger.error(f"No document found in data stream")
                        else:
                            self.tasks.add(self.executor.submit(self.put_worker, doc_id, document))
                            count += 1
                        buffer = buffer[position:]
                        buffer = buffer.lstrip()
                    except ValueError:
                        break
                self.task_wait(self.tasks)
                total += count
                self.calc_ops_per_sec(count)
                progress_count(total, errors=self.error_count, ops_per_sec=self.ops_per_sec)
            except IndexError:
                time.sleep(0.5)
        progress_count(total, finished=True, errors=self.error_count, ops_per_sec=self.ops_per_sec)

    def stream_out(self):
        writer = threading.Thread(target=self.to_file)
        writer.start()
        self.read_from_collection()
        writer.join()

    def stream_in(self):
        reader = threading.Thread(target=self.from_file)
        reader.start()
        self.write_to_collection()
        reader.join()

    def task_wait(self, tasks):
        result_set = []
        while tasks:
            done, tasks = concurrent.futures.wait(tasks, return_when=concurrent.futures.FIRST_COMPLETED)
            for task in done:
                try:
                    result = task.result()
                    if result:
                        result_set.append(result)
                except Exception as err:
                    with self._error_count.get_lock():
                        self._error_count.value += 1
                    logger.error(f"task error: {type(err).__name__}: {err}")
        return result_set

    @staticmethod
    def slice(iterable, batch_size):
        iterator = iter(iterable)
        while chunk := list(islice(iterator, batch_size)):
            yield chunk

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
