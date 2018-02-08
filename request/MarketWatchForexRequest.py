import json
from copy import deepcopy
from datetime import datetime
from pytz import timezone
from urllib import quote_plus

from MarketWatchForexRequestConstants import *

TIME_AND_STEP = {
    'PT1M': 'D10',
    'P1D': 'P10Y'
}

STEP_TRANSLATION = {
    '1m': 'PT1M',
    '1d': 'P1D'
}

class MarketWatchForexRequestException(Exception):
   pass

class MarketWatchForexRequest():
    def __init__(self, currency_pair, step_interval):
        if currency_pair not in CURRENCY_PAIRS:
            raise MarketWatchForexRequestException('Invalid currency pair supplied: {}'.format(currency_pair))
        if step_interval not in STEP_TRANSLATION.keys():
            raise MarketWatchForexRequestException('Invalid step interval supplied: {}'.format(step_interval))
        self.currency_pair = currency_pair
        self.step_interval = STEP_TRANSLATION[step_interval]

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
                raise MarketWatchForexRequestException('Time interval was not 1m or 1d: {}'.format(step))

            ticks = timeinfo['Ticks']
            series_data = {}
            for s in response['Series']:
                if s['Ticker']:
                    data['symbol'] = s['Ticker']
                if s['CommonName']:
                    data['common_name'] = s['CommonName']
                if s['TimeZoneData']:
                    data['time_zone'] = s['TimeZoneData']['StandardAbbreviation']
                for label in s['DesiredDataPoints']:
                    index = s['DesiredDataPoints'].index(label)
                    data_points = [x[index] for x in s['DataPoints']]
                    series_data[label] = data_points
        except Exception:
            return data

        if not all(len(x) == len(ticks) for x in series_data.values()):
            return data

        all_data = []
        for i, tick in enumerate(ticks):
            d = {}
            dt = datetime.fromtimestamp(tick/1000).replace(tzinfo=timezone(data['time_zone']))
            d['datetime'] = dt
            for key, val in series_data.iteritems():
                d[key] = val[i]
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
    today = datetime.now(timezone('EST')).date()
    today = datetime(year=today.year, month=today.month, day=today.day)
    a = MarketWatchForexRequest('AUDCAD', '1m')
    import requests
    from request.base.ResponseWrapper import ResponseWrapper
    req = requests.get(a.get_url(), headers=a.get_headers())
    response = ResponseWrapper(req)
    print MarketWatchForexRequest.parse_response(response.get_data())
