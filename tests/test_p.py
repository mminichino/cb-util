#!/usr/bin/env python3

import warnings
import sys
import argparse
import os
import logging
import string
import time
import asyncio
import traceback
import signal

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
sys.path.append(current)

from cbcmgr.cb_operation_s import CBOperation, Operation
from cbcmgr.mt_pool import CBPool
from cbcmgr.async_pool import CBPoolAsync
from conftest import pytest_sessionstart, pytest_sessionfinish

logger = logging.getLogger()

warnings.filterwarnings("ignore")
document = {
    "id": 1,
    "data": "data",
    "one": "one",
    "two": "two",
    "three": "tree"
}
new_document = {
    "id": 1,
    "data": "new",
    "one": "one",
    "two": "two",
    "three": "tree"
}
query_result = [
    {
        'data': 'data'
    }
]

json_data = {
            "name": "John Doe",
            "email": "jdoe@example.com",
            "addresses": {
                "billing": {
                    "line1": "123 Any Street",
                    "line2": "Anywhere",
                    "country": "United States"
                },
                "delivery": {
                    "line1": "123 Any Street",
                    "line2": "Anywhere",
                    "country": "United States"
                }
            },
            "history": {
                "events": [
                    {
                        "event_id": "1",
                        "date": "1/1/1970",
                        "type": "contact"
                    },
                    {
                        "event_id": "2",
                        "date": "1/1/1970",
                        "type": "contact"
                    }
                ]
            },
            "purchases": {
                "complete": [
                    339, 976, 442, 777
                ],
                "abandoned": [
                    157, 42, 999
                ]
            }
        }

xml_data = """<?xml version="1.0" encoding="UTF-8" ?>
<root>
  <name>John Doe</name>
  <email>jdoe@example.com</email>
  <addresses>
    <billing>
      <line1>123 Any Street</line1>
      <line2>Anywhere</line2>
      <country>United States</country>
    </billing>
    <delivery>
      <line1>123 Any Street</line1>
      <line2>Anywhere</line2>
      <country>United States</country>
    </delivery>
  </addresses>
  <history>
    <events>
      <event_id>1</event_id>
      <date>1/1/1970</date>
      <type>contact</type>
    </events>
    <events>
      <event_id>2</event_id>
      <date>1/1/1970</date>
      <type>contact</type>
    </events>
  </history>
  <purchases>
    <complete>339</complete>
    <complete>976</complete>
    <complete>442</complete>
    <complete>777</complete>
    <abandoned>157</abandoned>
    <abandoned>42</abandoned>
    <abandoned>999</abandoned>
  </purchases>
</root>
"""


class Params(object):

    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--ssl', action='store_true', help="Use SSL")
        parser.add_argument('--host', action='store', help="Hostname or IP address", default="127.0.0.1")
        parser.add_argument('--user', action='store', help="User Name", default="Administrator")
        parser.add_argument('--password', action='store', help="User Password", default="password")
        parser.add_argument('--bucket', action='store', help="Test Bucket", default="testrun")
        parser.add_argument('--start', action='store_true', help="Start Container")
        parser.add_argument('--stop', action='store_true', help="Stop Container")
        parser.add_argument("--external", action="store_true")
        parser.add_argument("--pool", action="store_true")
        parser.add_argument("--map", action="store_true")
        parser.add_argument("--perf", action="store_true")
        parser.add_argument("--verbose", action="store_true")
        parser.add_argument("--file", action="store", help="Input File")
        self.args = parser.parse_args()

    @property
    def parameters(self):
        return self.args


def container_start():
    pytest_sessionstart(None)


def container_stop():
    pytest_sessionfinish(None, 0)


def break_signal_handler(signum, frame):
    if 'DEBUG_LEVEL' in os.environ:
        if int(os.environ['DEBUG_LEVEL']) == 0:
            tb = traceback.format_exc()
            print(tb)
    print("")
    print("Break received, aborting.")
    sys.exit(0)


def unhandled_exception(_, context):
    err = context.get("exception", context['message'])
    if isinstance(err, Exception):
        print(f"unhandled exception: type: {err.__class__.__name__} msg: {err} cause: {err.__cause__}")
    else:
        print(f"unhandled error: {err}")


def perf_1(hostname, bucket, tls, scope, collection):
    ops = 100000
    print(f"=> Performance Test 1 ({ops} records)")
    pool = CBPool(hostname, "Administrator", "password", ssl=False, quota=128, create=True)
    print(f"  -> {pool.connections} connections, {pool.max_threads} threads")

    start_time = time.time()
    keyspace = f"{bucket}.{scope}.{collection}"
    pool.connect(keyspace)
    for i in range(ops):
        pool.dispatch(keyspace, Operation.WRITE, f"test::{i+1:06d}", document)
    pool.join()
    end_time = time.time()
    time_delta = end_time - start_time
    op_rate = ops / time_delta
    time_str = time.strftime("%H hours %M minutes %S seconds.", time.gmtime(time_delta))
    print(f"Test completed in {time_str} => rate {op_rate:.02f} ops/s")
    time.sleep(1)

    print(f"=> Test Cleanup")
    CBOperation(hostname, "Administrator", "password", ssl=tls).connect(keyspace).cleanup()
    time.sleep(1)


def perf_2(hostname, bucket, tls, scope, collection):
    ops = 100000
    collections = 10
    print(f"=> Performance Test 2 ({ops} records / {collections} collections)")
    pool = CBPool(hostname, "Administrator", "password", ssl=False, quota=128, create=True)
    print(f"  -> {pool.connections} connections, {pool.max_threads} threads")

    start_time = time.time()
    for n in range(ops):
        x = n % collections
        c = string.ascii_lowercase[x:x + 1]
        keyspace = f"{bucket}.{scope}.{collection}{c}"
        pool.connect(keyspace)
        pool.dispatch(keyspace, Operation.WRITE, f"test::{n+1:06d}", document)
    pool.join()
    end_time = time.time()
    time_delta = end_time - start_time
    op_rate = ops / time_delta
    time_str = time.strftime("%H hours %M minutes %S seconds.", time.gmtime(time_delta))
    print(f"Test completed in {time_str} => rate {op_rate:.02f} ops/s")
    time.sleep(1)

    print(f"=> Test Cleanup")
    keyspace = f"{bucket}.{scope}.{collection}a"
    CBOperation(hostname, "Administrator", "password", ssl=tls).connect(keyspace).cleanup()
    time.sleep(1)


async def perf_3(hostname, bucket, tls, scope, collection):
    ops = 100000
    print(f"=> Async Performance Test 3 ({ops} records)")
    pool = CBPoolAsync(hostname, "Administrator", "password", ssl=False, quota=128, create=True)

    start_time = time.time()
    keyspace = f"{bucket}.{scope}.{collection}"
    await pool.connect(keyspace)
    for i in range(ops):
        await pool.dispatch(keyspace, Operation.WRITE, f"test::{i+1:06d}", document)
    await pool.join()
    end_time = time.time()
    time_delta = end_time - start_time
    op_rate = ops / time_delta
    time_str = time.strftime("%H hours %M minutes %S seconds.", time.gmtime(time_delta))
    print(f"Test completed in {time_str} => rate {op_rate:.02f} ops/s")
    await asyncio.sleep(1)

    print(f"=> Test Cleanup")
    CBOperation(hostname, "Administrator", "password", ssl=tls).connect(keyspace).cleanup()
    await asyncio.sleep(1)


async def perf_4(hostname, bucket, tls, scope, collection):
    ops = 100000
    collections = 10
    print(f"=> Async Performance Test 4 ({ops} records / {collections} collections)")
    pool = CBPoolAsync(hostname, "Administrator", "password", ssl=False, quota=128, create=True)

    start_time = time.time()
    for n in range(ops):
        x = n % collections
        c = string.ascii_lowercase[x:x + 1]
        keyspace = f"{bucket}.{scope}.{collection}{c}"
        await pool.connect(keyspace)
        await pool.dispatch(keyspace, Operation.WRITE, f"test::{n+1:06d}", document)
    await pool.join()
    end_time = time.time()
    time_delta = end_time - start_time
    op_rate = ops / time_delta
    time_str = time.strftime("%H hours %M minutes %S seconds.", time.gmtime(time_delta))
    print(f"Test completed in {time_str} => rate {op_rate:.02f} ops/s")
    await asyncio.sleep(1)

    print(f"=> Test Cleanup")
    keyspace = f"{bucket}.{scope}.{collection}a"
    CBOperation(hostname, "Administrator", "password", ssl=tls).connect(keyspace).cleanup()
    await asyncio.sleep(1)


signal.signal(signal.SIGINT, break_signal_handler)
p = Params()
options = p.parameters

try:
    debug_level = int(os.environ['DEBUG_LEVEL'])
except (ValueError, KeyError):
    debug_level = 3

if debug_level == 0 or options.verbose:
    logger.setLevel(logging.DEBUG)
elif debug_level == 1:
    logger.setLevel(logging.ERROR)
elif debug_level == 2:
    logger.setLevel(logging.INFO)
else:
    logger.setLevel(logging.CRITICAL)

logging.basicConfig()

if options.start:
    container_start()
    sys.exit(0)

if options.stop:
    container_stop()
    sys.exit(0)

if options.perf:
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(unhandled_exception)
    perf_1(options.host, "test", options.ssl, "test", "test")
    perf_2(options.host, "test", options.ssl, "test", "test")
    loop.run_until_complete(perf_3(options.host, "test", options.ssl, "test", "test"))
    loop.run_until_complete(perf_4(options.host, "test", options.ssl, "test", "test"))
