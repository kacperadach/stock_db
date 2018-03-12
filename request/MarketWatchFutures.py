from bs4 import BeautifulSoup, SoupStrainer

BASE_URL = 'https://www.marketwatch.com/tools/futures'

class MarketWatchFutures():
    def __init__(self):
        pass

    def get_http_method(self):
        return 'GET'

    def get_url(self):
        return BASE_URL

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
    mwf = MarketWatchFutures()
    req = requests.get(mwf.get_url())
    print MarketWatchFutures.parse_response(req.content)
