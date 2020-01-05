from copy import deepcopy
from datetime import datetime
from urllib.parse import quote

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
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36'
}

DATA_HEADERS = {
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9,pl;q=0.8',
    'cache-control': 'no-cache',
    'origin': 'https://quotes.ino.com',
    'pragma': 'no-cache',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36'
}

CONTRACTS_BASE = 'https://quotes.ino.com/exchanges/contracts.html?r={}'

DATA_BASE = 'https://assets.ino.com/data/{}/?s={}&b={}&f=json'
DATA_REFERER = 'https://quotes.ino.com/charting/?s={}'

# 'https://assets.ino.com/data/minute/?s=NYMEX_SI.Z19&b=2019-10-15&f=json'
# 'https://assets.ino.com/data/history/?s=NYMEX_SI.Z19&b=&f=json'

DATA_PERIODS = (
    'history', #daily
    'hour',
    'minute'
)

MONTH_CODES = {
    'F': 'January',
    'G': 'February',
    'H': 'March',
    'J': 'April',
    'K': 'May',
    'M': 'June',
    'N': 'July',
    'Q': 'August',
    'U': 'September',
    'V': 'October',
    'X': 'November',
    'Z': 'December'
}



class InoFuturesRequestException(Exception):
    pass

class InoFuturesAllContractsRequest:

    def __init__(self):
        pass

    def get_url(self):
        return FUTURES_SYMBOLS

    def get_body(self):
        return {}

    def get_http_method(self):
        return 'GET'

    def get_headers(self):
        headers = deepcopy(HEADERS)
        headers['Referer'] = 'https://quotes.ino.com/exchanges/futboard/'
        return headers

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

def get_expiration(expiration_str):
    if expiration_str[0] not in MONTH_CODES:
        raise InoFuturesRequestException('month code not found')

    month = MONTH_CODES[expiration_str[0]]
    year = expiration_str[1:]
    date_str = '{}/{}/01'.format(year, month)
    return datetime.strptime(date_str, '%y/%B/%d')

class InoFuturesContractsRequest:

    def __init__(self, contract):
        self.contract = contract

    def get_url(self):
        return deepcopy(CONTRACTS_BASE).format(self.contract)

    def get_headers(self):
        headers = deepcopy(HEADERS)
        headers['Referer'] = 'https://quotes.ino.com/exchanges/futboard/'
        return headers

    def get_http_method(self):
        return 'GET'

    def get_body(self):
        return {}

    @staticmethod
    def parse_response(response):
        bs = BeautifulSoup(response, 'html.parser', parse_only=SoupStrainer('div', {'id': 'markets-menu'}))
        header = bs.find('h1').text
        name = header[:header.rindex('(')].strip()
        symbol = header.rsplit(':')[1].strip(')')

        bs = BeautifulSoup(response, 'html.parser', parse_only=SoupStrainer('div', {'class': 'table-responsive'}))

        contracts = []
        for row in bs.find_all('tr'):
            tds = row.find_all('td')
            if tds and len(tds) > 1:
                td = tds[1]
                contract_link = td.find('a').attrs['href'].split('=')[1]
                contract = tds[0].text

                if not contract.startswith(symbol + '.'):
                    raise InoFuturesRequestException('malformed future contract'.format(contract))

                if ':' in contract or '-' in contract:
                    expirations = contract[len(symbol) + 1:]
                    if ':' in contract:
                        delimiter = ':'
                    else:
                        delimiter = '-'
                    expirations = expirations.split(delimiter)

                    if '.' in expirations[1]:
                        expirations[1] = expirations[1].split('.')[1]

                    contracts.append({
                        'contract_link': contract_link,
                        'contract': contract,
                        'spread': True,
                        'date': get_expiration(expirations[0]),
                        'spread_date': get_expiration(expirations[1]),
                        'name': name
                    })
                else:
                    expiration_code = contract[len(symbol) + 1:]
                    expiration = get_expiration(expiration_code)
                    contracts.append({
                        'contract_link': contract_link,
                        'contract': contract,
                        'spread': False,
                        'date': expiration,
                        'spread_date': None,
                        'name': name
                    })
        return contracts



class InoFuturesRequest:

    # referer https://quotes.ino.com/charting/?s=NYMEX_SI.Z19

    def __init__(self, contract, period, start='', end=''):
        self.contract = contract
        self.period = period
        self.start = start
        self.end = end
        if start != '':
            try:
                datetime.strptime(start, '%Y-%m-%d')
            except Exception:
                raise InoFuturesRequestException('invalid date format')

        if period not in DATA_PERIODS:
            raise InoFuturesRequestException('invalid data period')

    def get_url(self):
        url = deepcopy(DATA_BASE).format(self.period, quote(self.contract.encode('utf-8')), self.start)
        if self.end != '' and self.period != 'minute':
            url += '&e={}'.format(self.end)
        return url

    def get_headers(self):
        headers = deepcopy(DATA_HEADERS)
        headers['referer'] = deepcopy(DATA_REFERER).format(self.contract)
        return headers

    def get_http_method(self):
        return 'GET'

    def get_body(self):
        return {}

    @staticmethod
    def parse_response(response):
        data = []
        for r in response:
            data.append({
                'timestamp_utc': r[0],
                'datetime_utc': datetime.utcfromtimestamp(r[0] / 1000),
                'open': r[1],
                'high': r[2],
                'low': r[3],
                'close': r[4],
                'volume': r[5] if len(r) >= 6 else 0
            })
        return data


FUTURE_OPTIONS_BASE = 'https://quotes.ino.com/options/?s={}'

class InoFuturesOptionsChainRequest:

    def __init__(self, contract):
        self.contract = contract

    def get_url(self):
        return deepcopy(FUTURE_OPTIONS_BASE).format(self.contract)

    def get_headers(self):
        headers = deepcopy(HEADERS)
        headers['referer'] = deepcopy(DATA_REFERER).format(self.contract)
        return headers

    def get_http_method(self):
        return 'GET'

    def get_body(self):
        return {}

    @staticmethod
    def parse_response(response):
        bs = BeautifulSoup(response, 'html.parser', parse_only=SoupStrainer('div', {'class': 'table-responsive'}))
        table = bs.find_all('table')[1]

        options = []
        for row in table.find_all('tr')[2:]:
            tds = row.find_all('td')
            symbol = row.find('a').text

            symbol_link = ''
            query_params = row.find('a').attrs['href'].split('?')[1].split('&')
            for qp in query_params:
                if qp.split('=')[0] == 's':
                    symbol_link = qp.split('=')[1]
                    break

            options.append({
                'expiration': datetime.strptime(tds[0].text, '%Y-%m-%d'),
                'strike': tds[1].text,
                'type': 'call' if symbol[-1] == 'C' else 'put',
                'symbol': symbol,
                'symbol_link': symbol_link
            })
        return options

#https://assets.ino.com/data/history/?s=NYMEX_SO.N20.1850C&b=&f=json

class InoFuturesOptionsRequest:

    def __init__(self, option, period):
        self.option = option



if __name__ == '__main__':
    import requests

    # request = InoFuturesAllContractsRequest()
    # response = requests.get(url=request.get_url(), headers=request.get_headers())
    # rw = ResponseWrapper(response)
    # all_contracts = InoFuturesAllContractsRequest.parse_response(rw.get_data())
    # for contract in all_contracts:
    #     request = InoFuturesContractsRequest(contract)
    #     response = requests.get(url=request.get_url(), headers=request.get_headers())
    #     rw = ResponseWrapper(response)
    #     contracts = InoFuturesContractsRequest.parse_response(rw.get_data())
    #     for c in contracts:

    # request = InoFuturesOptionsChainRequest('NYMEX_GC.F20')
    # response = requests.get(url=request.get_url(), headers=request.get_headers())
    # rw = ResponseWrapper(response)
    # data = InoFuturesOptionsChainRequest.parse_response(rw.get_data())

    # request = InoFuturesRequest('NYMEX_CL.H20', 'minute', '2019-10-29', '2019-12-31')
    # # request = InoFuturesRequest(c['contract'], 'hour', '2019-05-10')
    # response = requests.get(url=request.get_url(), headers=request.get_headers())
    # rw = ResponseWrapper(response)
    # data = InoFuturesRequest.parse_response(rw.get_data())
    # a = 1

    #ICE_@KRA
    request = InoFuturesRequest('NYMEX_SO.N20.1850C', 'hour')
    response = requests.get(url=request.get_url(), headers=request.get_headers())
    rw = ResponseWrapper(response)
    data = InoFuturesRequest.parse_response(rw.get_data())
    a = 1

