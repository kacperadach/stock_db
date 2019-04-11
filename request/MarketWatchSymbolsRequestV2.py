from copy import deepcopy

from bs4 import BeautifulSoup, SoupStrainer

from MarketWatchRequestConstants import COUNTRIES, INSTRUMENT_TYPES

BASE_URL = 'https://www.marketwatch.com/tools/markets/{}/country/{}/{}'

class MarketWatchSymbolsRequestV2Exception(Exception):
    pass

class MarketWatchSymbolsRequestV2():

    def __init__(self, country, page_number, instrument_type):
        if country not in COUNTRIES:
            raise MarketWatchSymbolsRequestV2Exception('invalid country')
        if page_number <= 0:
            raise MarketWatchSymbolsRequestV2Exception('page number must be 1 or greater')
        if instrument_type not in INSTRUMENT_TYPES:
            raise MarketWatchSymbolsRequestV2Exception('invalid instrument type')
        self.country = country
        self.page_number = page_number
        self.instrument_type = instrument_type

    def get_url(self):
        url = deepcopy(BASE_URL)
        return url.format(self.instrument_type, self.country, self.page_number)

    def get_http_method(self):
        return 'GET'

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
    request = MarketWatchSymbolsRequestV2('united-states', 1)
    req = requests.get(request.get_url())
    print MarketWatchSymbolsRequestV2.parse_response(req.content)