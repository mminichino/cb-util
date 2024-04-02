##
##

import logging
import time
import zlib
import json
import collections
import threading
from functools import partial
from cbcmgr.cb_operation_s import CBOperation

logger = logging.getLogger('cbutil.export.stream')
logger.addHandler(logging.NullHandler())


class StreamExport(CBOperation):

    def __init__(self, *args, keyspace: str, file_name: str, **kwargs):
        super().__init__(*args, **kwargs)
        self.connect(keyspace)
        self.queue = collections.deque()
        self.file_name = file_name
        self.compressor = zlib.compressobj(9, zlib.DEFLATED, zlib.MAX_WBITS | 16)
        self.decompressor = zlib.decompressobj(zlib.MAX_WBITS | 16)
        self.terminate = threading.Event()

    def to_file(self):
        buffer = bytearray()
        with open(self.file_name, 'wb') as zip_file:
            while not self.terminate.is_set() or self.queue:
                try:
                    record = self.queue.pop()
                    print(f"Read {len(record)}")
                    buffer.extend(record)
                    print(f"buffer size {len(buffer)}")
                except IndexError:
                    time.sleep(0.5)
                else:
                    if len(buffer) >= 131072:
                        chunk = buffer[:131072]
                        print(f"Zip {len(chunk)}")
                        zip_file.write(self.compressor.compress(chunk))
                        buffer = buffer[131072:]
            if len(buffer) > 0:
                print(f"Zip {len(buffer)}")
                zip_file.write(self.compressor.compress(buffer))
            zip_file.write(self.compressor.flush())

    def from_file(self):
        with open(self.file_name, 'rb') as zip_file:
            for chunk in iter(partial(zip_file.read, 131072), ''):
                if len(chunk) == 0:
                    break
                print(f"Read {len(chunk)}")
                part = self.decompressor.decompress(chunk)
                print(f"Decompressed {len(part)}")
                self.queue.appendleft(part)
        self.terminate.set()

    def read_from_collection(self):
        for doc_id in self.doc_list():
            document = self.get(doc_id)
            data = dict(
                doc_id=doc_id,
                document=document
            )
            block = json.dumps(data).encode('utf-8')
            self.queue.appendleft(block + b'\n')
            print(f"queued {len(block) + 1}")
        self.terminate.set()

    def write_to_collection(self):
        decoder = json.JSONDecoder()
        buffer = ''
        while not self.terminate.is_set() or self.queue:
            try:
                record = self.queue.pop()
                contents = record.decode('utf-8')
                buffer += contents
                print(f"Buffer {len(buffer)}")
                while buffer:
                    try:
                        json_object, position = decoder.raw_decode(buffer)
                        data = dict(json_object).copy()
                        doc_id = data.get('doc_id')
                        document = data.get('document', {})
                        if not doc_id or not document:
                            logger.error(f"No document found in data stream")
                            continue
                        self.put(doc_id, document)
                        buffer = buffer[position:]
                        buffer = buffer.lstrip()
                    except ValueError:
                        break
            except IndexError:
                time.sleep(0.5)

    def stream_out(self):
        reader = threading.Thread(target=self.read_from_collection)
        writer = threading.Thread(target=self.to_file)
        reader.start()
        writer.start()
        reader.join()
        writer.join()

    def stream_in(self):
        reader = threading.Thread(target=self.from_file)
        writer = threading.Thread(target=self.write_to_collection)
        reader.start()
        writer.start()
        reader.join()
        writer.join()
