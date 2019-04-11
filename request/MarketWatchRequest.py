import json
from copy import deepcopy
from datetime import datetime
from pytz import timezone
from urllib import quote_plus

from MarketWatchRequestConstants import *

TIME_AND_STEP = {
    'PT1M': 'D10',
    'P1D': 'P10Y'
}

STEP_TRANSLATION = {
    '1m': 'PT1M',
    '1d': 'P1D'
}

class MarketWatchRequestException(Exception):
   pass

class MarketWatchRequest():
    def __init__(self, symbol, instrument_type, step_interval):
        if instrument_type not in INSTRUMENT_TYPES:
            raise MarketWatchRequestException('invalid instrument_type')
        if step_interval not in STEP_TRANSLATION.keys():
            raise MarketWatchRequestException('Invalid step interval supplied: {}'.format(step_interval))
        self.instrument_type = instrument_type
        self.symbol = self.get_symbol(symbol, instrument_type)
        self.step_interval = STEP_TRANSLATION[step_interval]

    def get_http_method(self):
        return 'GET'

    def get_headers(self):
        return REQUEST_HEADER

    def get_url(self):
        query_params = deepcopy(QUERY_PARAMETER_JSON)
        query_params['Series'][0]['Key'] = self.symbol
        query_params['Step'] = self.step_interval
        query_params['TimeFrame'] = TIME_AND_STEP[self.step_interval]
        return GRAPH_URL.format(quote_plus(str(json.dumps(query_params)).replace(' ', '')))

    def get_symbol(self, symbol, instrument_type):
        if instrument_type.lower() == 'rates':
            if symbol['Country'] == 'Money Rates':
                return 'INTERSTATE/{}/{}/{}'.format(symbol['country_code'], symbol['exchange'], symbol['symbol'])
            else:
                return 'LOANRATE/{}/{}/{}'.format(symbol['country_code'], symbol['exchange'], symbol['symbol'])
        elif instrument_type.lower() == 'funds':
            return 'FUND/{}/{}/{}'.format(symbol['country_code'], symbol['exchange'], symbol['symbol'])
        elif instrument_type.lower() == 'bonds':
            return 'BOND/{}/{}/{}'.format(symbol['country_code'], symbol['exchange'], symbol['symbol'])
        elif instrument_type.lower() == 'benchmarks':
            return 'STOCK/{}/{}/{}'.format(symbol['country_code'], symbol['exchange'], symbol['symbol'])
        elif instrument_type.lower() == 'american-depository-receipt-stocks':
            return 'STOCK/{}/{}/{}'.format(symbol['country_code'], symbol['exchange'], symbol['symbol'])
        elif instrument_type.lower() == 'exchange-traded-notes':
            return 'STOCK/{}/{}/{}'.format(symbol['country_code'], symbol['exchange'], symbol['symbol'])
        elif instrument_type.lower() == 'warrants':
            return 'STOCK/{}/{}/{}'.format(symbol['country_code'], symbol['exchange'], symbol['symbol'])
        elif instrument_type.lower() == 'stocks':
            return 'STOCK/{}/{}/{}'.format(symbol['country_code'], symbol['exchange'], symbol['symbol'])
        elif instrument_type.lower() == 'indexes':
            return 'INDEX/{}/{}/{}'.format(symbol['country_code'], symbol['exchange'], symbol['symbol'])
        elif instrument_type.lower() == 'exchange-traded-funds':
            return 'FUND/{}/{}/{}'.format(symbol['country_code'], symbol['exchange'], symbol['symbol'])
        elif instrument_type.lower() == 'currencies':
            return 'CURRENCY/{}/{}/{}'.format(symbol['country_code'], symbol['exchange'], symbol['symbol'])
        elif instrument_type.lower() == 'crypto-currencies':
            return 'CRYPTOCURRENCY/{}/{}/{}'.format(symbol['country_code'], symbol['exchange'], symbol['symbol'])
        elif instrument_type.lower() == 'real-estate-investment-trusts':
            return 'STOCK/{}/{}/{}'.format(symbol['country_code'], symbol['exchange'], symbol['symbol'])
        else:
            raise MarketWatchRequestException('Invalid instrument type')

    @staticmethod
    def parse_response(response):
        data = {}
        data['data'] = []
        try:
            timeinfo = response['TimeInfo']
            step = timeinfo['Step']
            if step == 60000:
                data['time_interval'] = '1m'
            elif step == 86400000:
                data['time_interval'] = '1d'
            else:
                return {}

            ticks = timeinfo['Ticks']
            series_data = {}
            for s in response['Series']:
                if s['Ticker']:
                    data['symbol'] = s['Ticker']
                if s['CommonName']:
                    data['common_name'] = s['CommonName']
                if s['TimeZoneData']:
                    data['time_zone'] = s['TimeZoneData']['StandardAbbreviation']
                    data['utc_offset'] = s['UtcOffset']
                for label in s['DesiredDataPoints']:
                    index = s['DesiredDataPoints'].index(label)
                    data_points = [x[index] for x in s['DataPoints']]
                    series_data[label] = data_points
        except Exception:
            return {}

        if not all(len(x) == len(ticks) for x in series_data.values()) or 'utc_offset' not in data.keys():
            return {}

        all_data = []
        for i, tick in enumerate(ticks):
            d = {}
            dt = datetime.fromtimestamp((tick / 1000) + (data['utc_offset'] * 60))
            for key, val in series_data.iteritems():
                if val[i] is not None:
                    d[key] = val[i]
            if d:
                d['datetime'] = dt
                all_data.append(d)
        data['data'] = all_data
        return data

    @staticmethod
    def get_all_currency_pairs():
        from bs4 import BeautifulSoup
        import requests
        all_pairs = []
        for i in range(1, 50):
            try:
                req = requests.get(CURRENCY_PAIRS_URL.format(i))
                bs = BeautifulSoup(req.content, 'html.parser')
                html_rows = bs.find('table').find('tbody').find_all('tr')
                pairs = map(lambda x: x.find('small').text.strip(')').strip('('), html_rows)

                all_pairs += pairs
                print pairs
            except Exception:
                pass
        return all_pairs


if __name__ == '__main__':
    a = MarketWatchRequest('aapl', '1d')
    i = 1