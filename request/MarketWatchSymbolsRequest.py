import string
from copy import deepcopy

from bs4 import BeautifulSoup, SoupStrainer

BASE_URL = 'https://www.marketwatch.com/tools/markets/{}/a-z/{}/{}'
TYPE_OPTIONS = (
    'stocks',
    'rates',
    'funds',
    'bonds',
    'benchmarks',
    'real-estate-investment-trusts',
    'futures',
    'american-depository-receipt-stocks',
    'exchange-traded-notes',
    'warrants',
    'indexes',
    'exchange-traded-funds',
    'currencies',
    'crypto-currencies'
)

class MarketWatchSymbolsRequestException(Exception):
    pass

class MarketWatchSymbolsRequest():

    def __init__(self, type, letter, page=1):
        if type not in TYPE_OPTIONS:
            raise MarketWatchSymbolsRequestException('invalid type: {}'.format(type))
        self.type = type
        self.letter = letter
        self.page = page

    def get_url(self):
        url = deepcopy(BASE_URL)
        return url.format(self.type, self.letter, self.page)

    def get_http_method(self):
        return 'GET'

    @staticmethod
    def get_letter_options():
        return list(string.ascii_lowercase) + ['0-9', 'Other']

    @staticmethod
    def parse_response(response):
        bs = BeautifulSoup(response, 'html.parser', parse_only=SoupStrainer('table'))
        headers = list(bs.find_all('th'))
        rows = bs.find_all('tr')
        symbols = []
        for row in rows:
            if len(row.find_all('th')) != 0:
                continue
            document = {}
            tds = row.find_all('td')
            for i, td in enumerate(tds):
                if len(td.find_all('a')) != 0:
                    symbol = td.find('small').text.strip(')').strip('(')
                    long_name = td.text[0:td.text.rfind('(')].strip()
                    document['symbol'] = symbol
                    document['long_name'] = long_name
                    url = td.find('a').attrs['href']
                    query_params = url.split('?')[1].split('&')
                    for query_param in query_params:
                        key, value = query_param.split('=')
                        document[key] = value
                else:
                    document[headers[i].text] = td.text
            symbols.append(document)
        return symbols

if __name__ == '__main__':
    import requests
    for type in TYPE_OPTIONS:
        mwf = MarketWatchSymbolsRequest(type, 'A')
        req = requests.get(mwf.get_url())
        if MarketWatchSymbolsRequest.parse_response(req.content):
            print type + ': ' + str(MarketWatchSymbolsRequest.parse_response(req.content)[0])
