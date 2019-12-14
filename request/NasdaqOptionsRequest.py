from copy import deepcopy
from datetime import datetime

from request.base.ResponseWrapper import ResponseWrapper

BASE = 'https://api.nasdaq.com/api/quote/{}/option-chain?assetclass={}&limit=0'

BASE_REFERER = 'https://www.nasdaq.com/market-activity/{}/{}/option-chain'
FUNDS_AND_ETFS = 'funds-and-etfs'
STOCKS = 'stocks'
#
# 'https://www.nasdaq.com/market-activity/funds-and-etfs/gdx/option-chain'

'https://api.nasdaq.com/api/quote/GDX/option-chain?assetclass=etf&limit=6'

STOCK_ASSET_CLASS = 'stocks'
ETF_ASSET_CLASS = 'etf'

HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9,pl;q=0.8',
    'cache-control': 'no-cache',
    'origin': 'https://www.nasdaq.com',
    'pragma': 'no-cache',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site'
}




class NasdaqOptionsRequestException(Exception):
    pass

class NasdaqOptionsRequest:

    def __init__(self, symbol, instrument_type):
        self.symbol = symbol
        self.instrument_type = instrument_type
        if instrument_type == 'stocks':
            self.asset_class = STOCK_ASSET_CLASS
        else:
            self.asset_class = ETF_ASSET_CLASS

    def get_http_method(self):
        return 'GET'

    def get_headers(self):
        headers = deepcopy(HEADERS)
        type = STOCKS if self.instrument_type == 'stocks' else FUNDS_AND_ETFS
        referer = deepcopy(BASE_REFERER).format(type, self.symbol)
        headers['referer'] = referer
        return headers

    def get_url(self):
        return deepcopy(BASE).format(self.symbol.lower(), self.asset_class)

    @staticmethod
    def parse_response(response):
        data = {}
        if 'data' not in response.keys():
            return data

        if 'optionChainList' not in response['data'].keys() or 'rows' not in response['data']['optionChainList'].keys() or response['data']['optionChainList']['rows'] is None:
            return data


        options = {}
        for row in response['data']['optionChainList']['rows']:

            if 'call' in row.keys() and row['call'] is not None:
                call = row['call']
                d = str(datetime.strptime(call['expiryDate'], '%m/%d/%Y').date())
                call['expiryDate'] = d
                if d not in options.keys():
                    options[d] = {'calls': [call], 'puts': []}
                else:
                    options[d]['calls'].append(call)

            if 'put' in row.keys() and row['put'] is not None:
                put = row['put']
                d = str(datetime.strptime(put['expiryDate'], '%m/%d/%Y').date())
                put['expiryDate'] = d
                if d not in options.keys():
                    options[d] = {'calls': [], 'puts': [put]}
                else:
                    options[d]['puts'].append(put)

        return options


if __name__ == '__main__':
    r = NasdaqOptionsRequest('AAPL', 'stocks')
    import requests
    response = requests.get(r.get_url(), headers=r.get_headers())
    rw = ResponseWrapper(response)
    NasdaqOptionsRequest.parse_response(rw.get_data())