from copy import deepcopy
from datetime import datetime, timedelta
from pytz import timezone

# https://calendar-api.fxstreet.com/en/api/v1/eventDates/2019-11-24T00:00:00Z/2019-11-29T23:59:59Z?&countries=DE&countries=AU&countries=CA&countries=CN&countries=EMU&countries=DE&countries=ES&countries=US&countries=FR&countries=IT&countries=JP&countries=NZ&countries=UK&countries=CH
from core.data.SnakeCase import SnakeCase
from request.base.ResponseWrapper import ResponseWrapper

CALENDAR_BASE = 'https://calendar-api.fxstreet.com'
CALENDAR_PATH = '/en/api/v1/eventDates/{}/{}?&countries=DE&countries=AU&countries=CA&countries=CN&countries=EMU&countries=DE&countries=ES&countries=US&countries=FR&countries=IT&countries=JP&countries=NZ&countries=UK&countries=CH'

HEADERS = {
    'authority': 'calendar-api.fxstreet.com',
    'method': 'GET',
    'scheme': 'https',
    'accept': '*/*',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9,pl;q=0.8',
    'cache-control': 'no-cache',
    'origin': 'https://www.fxstreet.com',
    'pragma': 'no-cache',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36'
}

CALENDAR_REFERER = 'https://www.fxstreet.com/economic-calendar'


# :authority: calendar-api.fxstreet.com
# :method: GET
# :path: /en/api/v1/events/38ec9435-34cc-4704-9445-80fabf6c0120/upcoming?take=1
# :scheme: https
# accept: */*
# accept-encoding: gzip, deflate, br
# accept-language: en-US,en;q=0.9,pl;q=0.8
# cache-control: no-cache
# origin: https://www.fxstreet.com
# pragma: no-cache
# referer: https://www.fxstreet.com/economic-calendar/event/38ec9435-34cc-4704-9445-80fabf6c0120?timezoneOffset=0
# sec-fetch-mode: cors
# sec-fetch-site: same-site
# user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36


# https://www.fxstreet.com/economic-calendar
# https://www.fxstreet.com/economic-calendar/event/c2eae72d-8620-4bed-8f99-838be757d023?timezoneOffset=0

# https://calendar-api.fxstreet.com/en/api/v1/events/38ec9435-34cc-4704-9445-80fabf6c0120/historical
# https://calendar-api.fxstreet.com/en/api/v1/events/38ec9435-34cc-4704-9445-80fabf6c0120/upcoming

# https://calendar-api.fxstreet.com/en/api/v1/marketImpact/38ec9435-34cc-4704-9445-80fabf6c0120/trueRange/fxs-3212164
# https://calendar-api.fxstreet.com/en/api/v1/marketImpact/38ec9435-34cc-4704-9445-80fabf6c0120/volatility/fxs-3212164

class FxstreetRequestException(Exception):
    pass

class FxstreetRequest:

    def __init__(self, start, end):
        if not isinstance(start, datetime):
            raise FxstreetRequestException('start needs to be datetime')

        if not isinstance(end, datetime):
            raise FxstreetRequestException('end needs to be datetime')

        self.start = start
        self.end = end

    def get_http_method(self):
        return 'GET'

    def get_headers(self):
        headers = deepcopy(HEADERS)
        headers['path'] = CALENDAR_PATH.format(self.start.strftime('%Y-%m-%dT%H:%M:%SZ'), self.end.strftime('%Y-%m-%dT%H:%M:%SZ'))
        headers['referer'] = CALENDAR_REFERER
        return headers

    def get_url(self):
        return CALENDAR_BASE + CALENDAR_PATH.format(self.start.strftime('%Y-%m-%dT%H:%M:%SZ'), self.end.strftime('%Y-%m-%dT%H:%M:%SZ'))

    @staticmethod
    def parse_response(response):
        data = []
        for r in response:
            event = {}
            for key, value in r.items():
                if key == 'dateUtc':
                    try:
                        event['datetime_utc'] = timezone('UTC').localize(datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ'))
                    except Exception:
                        event['datetime_utc'] = timezone('UTC').localize(datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ'))
                else:
                    event[SnakeCase.to_snake_case(key)] = value
            data.append(event)
        return data


if __name__ == '__main__':
    request = FxstreetRequest(datetime.now() - timedelta(days=100), datetime.now() - timedelta(days=0))
    import requests
    response = ResponseWrapper(requests.get(request.get_url(), headers=request.get_headers()))
    d = FxstreetRequest.parse_response(response.get_data())
    a = 1
