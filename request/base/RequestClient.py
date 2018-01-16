import json

import requests
from requests.exceptions import ChunkedEncodingError
from requests.exceptions import ReadTimeout
from requests import ConnectionError
from fake_useragent import UserAgent
from copy import deepcopy

from core.StockDbBase import StockDbBase
from request.base.ResponseWrapper import ResponseWrapper

HEADERS = {
    'content-type': "application/json",
    'cache-control': "no-cache",
    'User-Agent': None
}

TOR_PROXIES = {
    'http': 'socks5h://localhost:{}',
    'https': 'socks5h://localhost:{}'
}

MAX_RETRIES = 2
TIMEOUT = 10

class RequestClientException(Exception):
    pass

class RequestClient(StockDbBase):

    def __init__(self, use_tor=False, tor_client=None):
        if use_tor and not tor_client:
            raise RequestClientException('use_tor was set to True but no tor_client was supplied')
        self.ua = UserAgent()
        self.use_tor = use_tor
        self.max_retries = MAX_RETRIES
        self.tor_client = tor_client
        if use_tor:
            self.tor_client.connect()

    def _get_headers(self):
        headers = HEADERS
        headers['User-Agent'] = self.ua.random
        return headers

    def _get_proxies(self):
        if not self.use_tor:
            return {}
        proxies = deepcopy(TOR_PROXIES)
        for key, value in proxies.iteritems():
            proxies[key] = value.format(self.tor_client.SocksPort)
        return proxies

    def get(self, url):
        response = None
        retries = 0
        while retries < self.max_retries:
            try:
                response = requests.get(url.strip(), headers=self._get_headers(), proxies=self._get_proxies(), timeout=5)
            except ConnectionError:
                self.log('ConnectionError occurred')
                response = None
            except ChunkedEncodingError:
                self.log('ChunkedEncodingError occurred')
                response = None
            except ReadTimeout:
                self.log('{} timed out after {} seconds'.format(url, TIMEOUT))
                response = None
            retries += 1

            if response is not None and response.status_code == 200:
                break
            if self.use_tor:
                self.tor_client.new_nym()
        return ResponseWrapper(response)

    def post(self, url, data):
        response = None
        retries = 0
        while retries < self.max_retries:
            try:
                response = requests.post(url.strip(), headers=self._get_headers(), proxies=self._get_proxies(), data=json.dumps(data))
            except requests.ConnectionError:
                self.log('ConnectionError occurred')
                response = None
            retries += 1

            if response is not None and response.status_code == 200:
                break
            if self.use_tor:
                self.tor_client.new_nym()
        return ResponseWrapper(response)
