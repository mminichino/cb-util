##
##

import logging
import math
import json
import os
import requests
import warnings
import base64
import asyncio
from typing import Union
from requests.adapters import HTTPAdapter, Retry
from requests.auth import AuthBase
from aiohttp import ClientSession
from cbcmgr.retry import retry

logger = logging.getLogger('cbcmgr.restmgr')
logger.addHandler(logging.NullHandler())
logging.getLogger("urllib3").setLevel(logging.CRITICAL)
logging.getLogger("asyncio").setLevel(logging.CRITICAL)


class CapellaAuth(AuthBase):

    def __init__(self, key_file: Union[str, None] = None):
        _key_file = key_file if key_file is not None else "default-api-key-token.txt"
        _credential_file = os.path.join(os.environ['HOME'], '.capella', key_file)
        _profile_token = None
        _profile_key_id = None
        self.profile_token = None
        self.profile_key_id = None

        if os.path.exists(_credential_file):
            try:
                credential_data = dict(line.split(':', 1) for line in open(_credential_file))
                _profile_token = credential_data.get('APIKeyToken')
                if _profile_token:
                    _profile_token = _profile_token.strip()
                    _profile_key_id = credential_data.get('APIKeyId', '').strip()
            except Exception as err:
                raise Exception(f"can not read credential file {_credential_file}: {err}")

        if 'CAPELLA_TOKEN' in os.environ:
            self.profile_token = os.environ['CAPELLA_TOKEN']
        elif _profile_token:
            self.profile_token = _profile_token
            self.profile_key_id = _profile_key_id
        else:
            raise Exception("Please set Capella Token for Capella API access (for example in $HOME/.capella/default-api-key-token.txt)")

        logger.debug(f"APIKeyId: {self.profile_key_id}")
        self.request_headers = {
            "Authorization": f"Bearer {self.profile_token}",
        }

    def __call__(self, r):
        r.headers.update(self.request_headers)
        return r

    def get_header(self):
        return self.request_headers


class BasicAuth(AuthBase):

    def __init__(self, username, password):
        self.username = username
        self.password = password
        auth_hash = f"{self.username}:{self.password}"
        auth_bytes = auth_hash.encode('ascii')
        auth_encoded = base64.b64encode(auth_bytes)

        self.request_headers = {
            "Authorization": f"Basic {auth_encoded.decode('ascii')}",
        }

    def __call__(self, r):
        r.headers.update(self.request_headers)
        return r

    def get_header(self):
        return self.request_headers


class RESTManager(object):

    def __init__(self,
                 hostname: str,
                 username: Union[str, None] = None,
                 password: Union[str, None] = None,
                 token: Union[str, None] = None,
                 ssl: bool = True,
                 verify: bool = True,
                 port: Union[int, None] = None,
                 key_file: Union[str, None] = None):
        warnings.filterwarnings("ignore")
        self.hostname = hostname
        self.username = username
        self.password = password
        self.token = token
        self.ssl = ssl
        self.verify = verify
        self.port = port
        self.scheme = 'https' if self.ssl else 'http'
        self.key_file = key_file if key_file is not None else "default-api-key-token.txt"
        self.response_text = None
        self.response_list = []
        self.response_dict = {}
        self.response_code = 200
        self.loop = asyncio.get_event_loop()

        if self.username is not None and self.password is not None:
            self.auth_class = BasicAuth(self.username, self.password)
        else:
            logger.debug(f"Using Capella key file: {self.key_file}")
            self.auth_class = CapellaAuth(self.key_file)

        self.request_headers = self.auth_class.get_header()
        self.session = requests.Session()
        retries = Retry(total=10,
                        backoff_factor=0.01)
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

        if not port:
            if ssl:
                self.port = 443
            else:
                self.port = 80

        self.url_prefix = f"{self.scheme}://{self.hostname}:{self.port}"

    def get(self, url: str):
        response = self.session.get(url, auth=self.auth_class, verify=self.verify)
        self.response_text = response.text
        self.response_code = response.status_code
        return self

    def validate(self):
        if self.response_code >= 300:
            try:
                response_json = json.loads(self.response_text)
                message = f"Can not access Capella API: Response Code: {self.response_code}"
                if 'hint' in response_json:
                    message += f" Hint: {response_json['hint']}"
                raise RuntimeError(message)
            except json.decoder.JSONDecodeError:
                raise RuntimeError(f"Invalid response from API endpoint: response code: {self.response_code}")
        return self

    def json(self):
        return json.loads(self.response_text)

    def list(self):
        return self.response_list

    def filter(self, key: str, value: str):
        self.response_list = [item for item in self.response_list if item.get(key) == value]
        return self

    def default(self):
        try:
            self.response_dict = self.response_list[0]
        except IndexError:
            self.response_dict = {}
        return self

    def item(self, index: int):
        try:
            self.response_dict = self.response_list[index]
        except IndexError:
            self.response_dict = {}
        return self

    def key(self, key: str):
        return self.response_dict.get(key)

    def record(self):
        return self.response_dict

    def by_name(self, name: str):
        self.response_list = [item for item in self.response_list if item.get('name') == name]
        return self

    def by_id(self, item_id: str):
        self.response_list = [item for item in self.response_list if item.get('id') == item_id]
        return self

    def name(self):
        try:
            return self.response_dict['name']
        except KeyError:
            return None

    def id(self):
        try:
            return self.response_dict['id']
        except KeyError:
            return None

    def unique(self):
        if len(self.response_list) != 1:
            raise ValueError("More than one object matches search criteria")
        return self.default()

    def page_url(self, endpoint: str, page: int, per_page: int) -> str:
        return f"{self.url_prefix}/{endpoint}?page={page}&perPage={per_page}"

    async def get_async(self, url: str):
        async with ClientSession(headers=self.request_headers) as session:
            async with session.get(url, verify_ssl=self.verify) as response:
                response = await response.json()
                return response.get('data', [])

    async def get_kv_async(self, url: str, key: str, value: str):
        async with ClientSession(headers=self.request_headers) as session:
            async with session.get(url, verify_ssl=self.verify) as response:
                response = await response.json()
                return [item for item in response.get('data', []) if item.get(key) == value]

    async def get_capella_a(self, endpoint: str):
        data = []
        url = self.page_url(endpoint, 1, 1)
        cursor = self.get(url).validate().json()

        total_items = cursor.get('cursor', {}).get('pages', {}).get('totalItems', 1)
        pages = math.ceil(total_items / 10)

        for result in asyncio.as_completed([self.get_async(self.page_url(endpoint, page, 10)) for page in range(1, pages + 1)]):
            block = await result
            data.extend(block)

        self.response_list = data

    @retry()
    async def get_capella_kv_a(self, endpoint: str, key: str, value: str):
        data = []
        url = self.page_url(endpoint, 1, 1)
        cursor = self.get(url).validate().json()

        total_items = cursor.get('cursor', {}).get('pages', {}).get('totalItems', 1)
        pages = math.ceil(total_items / 10)

        for result in asyncio.as_completed([self.get_kv_async(self.page_url(endpoint, page, 10), key, value) for page in range(1, pages + 1)]):
            block = await result
            data.extend(block)

        if len(data) == 0:
            raise ValueError('No match')
        self.response_list = data

    def get_capella(self, endpoint: str):
        self.loop.run_until_complete(self.get_capella_a(endpoint))
        return self

    def get_capella_kv(self, endpoint: str, key: str, value: str):
        self.loop.run_until_complete(self.get_capella_kv_a(endpoint, key, value))
        return self
