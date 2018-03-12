from copy import deepcopy

from MarketWatchRequestConstants import *

class MarketWatchFuturesRequest():

    def __init__(self, futures_symbol, step_interval):
        self.futures_symbol = futures_symbol
        self.step_interval = step_interval

    def get_http_method(self):
        return 'GET'

    def get_headers(self):
        return REQUEST_HEADER

    def get_url(self):
        query_params = deepcopy(QUERY_PARAMETER_JSON)
        key = query_params['Series'][0]['Key']
        query_params['Series'][0]['Key'] = key.format(self.currency_pair)
        query_params['Step'] = self.step_interval
        query_params['TimeFrame'] = TIME_AND_STEP[self.step_interval]
        return CURRENCY_URL.format(quote_plus(str(json.dumps(query_params)).replace(' ', '')))