from copy import deepcopy
from datetime import datetime

from request.base.ResponseWrapper import ResponseWrapper

from bs4 import SoupStrainer, BeautifulSoup

FUTURES_SYMBOLS = 'https://quotes.ino.com/exchanges/futboard/'

HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9,pl;q=0.8',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Host': 'quotes.ino.com',
    'Pragma': 'no-cache',
    'Referer': 'https://quotes.ino.com/exchanges/futboard/',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36'
}

CONTRACTS_BASE = 'https://quotes.ino.com/exchanges/contracts.html?r={}'

DATA_BASE = 'https://assets.ino.com/data/{}/?s={}&b={}&f=json'

# 'https://assets.ino.com/data/minute/?s=NYMEX_SI.Z19&b=2019-10-15&f=json'
# 'https://assets.ino.com/data/history/?s=NYMEX_SI.Z19&b=&f=json'

DATA_PERIODS = (
    'history', #daily
    'hour',
    'minute'
)


class InoFuturesRequestException(Exception):
    pass

class InoFuturesAllContractsRequest:

    def __init__(self):
        pass

    def get_url(self):
        return FUTURES_SYMBOLS

    def get_http_method(self):
        return 'GET'

    def get_headers(self):
        return HEADERS

    @staticmethod
    def parse_response(response):
        bs = BeautifulSoup(response, 'html.parser', parse_only=SoupStrainer('div', {'class': 'table-responsive'}))

        contracts = []
        for row in bs.find_all('tr'):
            tds = row.find_all('td')
            if tds:
                td = tds[-1]
                links = td.find_all('a')
                if links:
                    contract = links[0].attrs['href'].split('=')[1]
                    contracts.append(contract)
        return contracts

class InoFuturesContractsRequest:

    def __init__(self, contract):
        self.contract = contract

    def get_url(self):
        return deepcopy(CONTRACTS_BASE).format(self.contract)

    def get_headers(self):
        return HEADERS

    def get_http_method(self):
        return 'GET'

    @staticmethod
    def parse_response(response):
        bs = BeautifulSoup(response, 'html.parser', parse_only=SoupStrainer('div', {'id': 'markets-menu'}))
        name = bs.find('h1').text
        name = name[:name.index('(')].strip()

        bs = BeautifulSoup(response, 'html.parser', parse_only=SoupStrainer('div', {'class': 'table-responsive'}))

        contracts = []
        for row in bs.find_all('tr'):
            tds = row.find_all('td')
            if tds and len(tds) > 1:
                td = tds[1]
                contract = td.find('a').attrs['href'].split('=')[1]
                try:
                    date = datetime.strptime(td.text, '%b %Y')
                    contracts.append({
                        'contract': contract,
                        'spread': False,
                        'date': date,
                        'spread_date': None,
                        'name': name
                    })
                except Exception:
                    spread_months = td.text.strip(name).split('/')
                    first = datetime.strptime(spread_months[0], '%b %y')
                    second = datetime.strptime(spread_months[1], '%b %y')
                    contracts.append({
                        'contract': contract,
                        'spread': True,
                        'date': first,
                        'spread_date': second,
                        'name': name
                    })
        return contracts



class InoFuturesRequest:

    # referer https://quotes.ino.com/charting/?s=NYMEX_SI.Z19

    def __init__(self, contract, period, start):
        self.contract = contract
        self.period = period
        self.start = start
        if period not in DATA_PERIODS:
            raise InoFuturesRequestException('invalid data period')



if __name__ == '__main__':
    import requests

    request = InoFuturesAllContractsRequest()
    response = requests.get(url=request.get_url(), headers=request.get_headers())
    rw = ResponseWrapper(response)
    contracts = InoFuturesAllContractsRequest.parse_response(rw.get_data())
    for c in contracts:
        request = InoFuturesContractsRequest(c)
        response = requests.get(url=request.get_url(), headers=request.get_headers())
        rw = ResponseWrapper(response)
        InoFuturesContractsRequest.parse_response(rw.get_data())
