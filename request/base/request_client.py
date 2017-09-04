import requests
from fake_useragent import UserAgent

from app.config import App_Config
from request.base.response_wrapper import ResponseWrapper

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

class RequestClient():

    def __init__(self):
        self.ua = UserAgent()
        self.use_tor = App_Config.use_tor

    def _get_headers(self):
        headers = HEADERS
        headers['User-Agent'] = self.ua.random
        return headers

    def _get_proxies(self):
        if self.use_tor:
            return TOR_PROXIES
        else:
            return {}

    def get(self, url):
        try:
            response = requests.get(url.strip(), headers=self._get_headers(), proxies=self._get_proxies())
        except requests.ConnectionError:
            response = None
        return ResponseWrapper(response)

    def post(self, url, data):
        try:
            response = requests.post(url.strip(), headers=self._get_headers(), proxies=self._get_proxies(), data=data)
        except requests.ConnectionError:
            response = None
        return ResponseWrapper(response)
