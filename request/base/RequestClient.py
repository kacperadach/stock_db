import requests
from requests.exceptions import ChunkedEncodingError
from requests.exceptions import ReadTimeout
from requests import ConnectionError
from fake_useragent import UserAgent
from threading import Lock

from core.StockDbBase import StockDbBase
from request.base.ResponseWrapper import ResponseWrapper
from request.base.TorClient import Tor_Client

HEADERS = {
    'content-type': "application/json",
    'cache-control': "no-cache",
    'User-Agent': None
}

TOR_PROXIES = {
    'http': 'socks5://localhost:9050',
    'https': 'socks5://localhost:9050'
}

CONTROLLER_PORT = 9051
MAX_RETRIES = 2
MIN_REQUEST_PER_NYM = 50
TIMEOUT = 5

class RequestClient(StockDbBase):

    def __init__(self, use_tor=False):
        self.ua = UserAgent()
        self.use_tor = use_tor
        self.max_retries = MAX_RETRIES
        self.tor_client = Tor_Client
        self.requests_made = 0
        self.lock = Lock()
        if use_tor:
            self.tor_client.connect()

    def _get_headers(self):
        headers = HEADERS
        headers['User-Agent'] = self.ua.random
        return headers

    def _get_proxies(self):
        return TOR_PROXIES if self.use_tor else {}

    def get(self, request_item):
        retries = 0
        while retries < self.max_retries:
            try:
                response = requests.get(request_item.url.strip(), headers=self._get_headers(), proxies=self._get_proxies(), timeout=5)
            except ConnectionError:
                self.log('ConnectionError occurred')
                response = None
            except ChunkedEncodingError:
                self.log('ChunkedEncodingError occurred')
                response = None
            except ReadTimeout:
                self.log('{} timed out after {} seconds'.format(request_item.url, TIMEOUT))
                response = None
            self.requests_made += 1
            retries += 1

            if response is not None and response.status_code == 200:
                break
            if self.use_tor and self.requests_made > MIN_REQUEST_PER_NYM:
                new_nym = self.tor_client.new_nym()
                if new_nym:
                    self.requests_made = 0
        return ResponseWrapper(response)

    def post(self, url, data):
        try:
            response = requests.post(url.strip(), headers=self._get_headers(), proxies=self._get_proxies(), data=data)
        except requests.ConnectionError:
            response = None
        return ResponseWrapper(response)
