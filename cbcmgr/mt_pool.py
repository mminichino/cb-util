##
##

import concurrent.futures


class CBPool(object):

    def __init__(self, hostname: str, username: str, password: str, ssl=False, external=False, kv_timeout: int = 5, query_timeout: int = 60):
        self.tasks = set()
        self.executor = concurrent.futures.ThreadPoolExecutor()
