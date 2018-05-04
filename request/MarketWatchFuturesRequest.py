from BaseRequest import BaseRequest

from bs4 import BeautifulSoup, SoupStrainer

FUTURES_URL = 'https://www.marketwatch.com/tools/futures'
FUTURES_MARKETS_URL = 'https://www.marketwatch.com/tools/markets/futures'

class MarketWatchFuturesRequestException(Exception):
    pass

class MarketWatchFuturesRequest(BaseRequest):

    def __init__(self, type):
        if type.lower() not in ('symbols', 'markets'):
            raise MarketWatchFuturesRequestException('invalid type')
        self.type = type.lower()

    def get_http_method(self):
        return 'GET'

    def get_url(self):
        return FUTURES_URL if self.type == 'symbols' else FUTURES_MARKETS_URL

    @staticmethod
    def parse_response(response):
        bs = BeautifulSoup(response, 'html.parser', parse_only=SoupStrainer('table', {'id': 'futuresContracts'}))
        futures_symbols = []
        for row in bs.find_all('tr'):
            tds = row.find_all('td')
            if not tds:
                continue
            url = tds[0].find('a').attrs['href']
            temp = url.split('/')[3]
            if '?' not in temp:
                symbol = temp
            else:
                symbol, _ = temp.split('?')
            long_name = tds[0].find('a').text
            futures_symbols.append({'symbol': symbol, 'longName': long_name})
        return futures_symbols

if __name__ == '__main__':
    import requests
    mwf = MarketWatchFuturesRequest()
    req = requests.get(mwf.get_url())
    print MarketWatchFuturesRequest.parse_response(req.content)
