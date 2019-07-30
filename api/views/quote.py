from datetime import datetime, timedelta, date

import math
from time import sleep

from pytz import timezone

from acquisition.symbol.futures import FUTURES
from core.data.QuoteRepository import Quote_Repository

from cachetools import cached, TTLCache

DELTA_MAP = {
    '15m': 5,
    '1d': 60,
    '1w': 365
}

cache = TTLCache(maxsize=1000, ttl=15)


@cached(cache)
def fetch_live_futures_quotes():
    print 'fuck off'
    symbols = map(lambda x: x['symbol'], FUTURES)
    return Quote_Repository.get_live_quotes('market_watch_futures', symbols, '1d')

def get_live_futures_quotes():
    return fetch_live_futures_quotes().values()

# def get_live_futures_quotes(offset, limit):
#     cached_data = fetch_live_futures_quotes()
#     return cached_data.values()[offset:offset+limit]

def get_quote(instrument_type, exchange, symbol, time_interval='1d', start=None, end=None, limit=None):
    now = datetime.now(timezone('EST'))

    start_date = now - timedelta(days=DELTA_MAP[time_interval])
    if start is not None:
        if isinstance(start, date):
            start_date = datetime.combine(start, datetime.min.time())
        else:
            start_date = datetime.strptime(start, '%Y-%m-%d')

    end_date = now
    if end is not None:
        if isinstance(end, date):
            end_date = datetime.combine(end, datetime.min.time())
        else:
            end_date = datetime.strptime(end, '%Y-%m-%d') - timedelta(days=1)

    data = Quote_Repository.get_interval(instrument_type, exchange, symbol, start_date, end_date, time_interval, limit=limit)
    if not data['data'] and time_interval != '1d':
        if time_interval in ('15m', '15M'):
            data = Quote_Repository.get_interval(instrument_type, exchange, symbol, start_date, end_date, '1m')

    Quote_Repository.request_quote(instrument_type, exchange, symbol)
    return data

if __name__ == '__main__':
    for i in range(100):
        for x in range(int(math.ceil(len(FUTURES) / 2))):
            print x
            get_live_futures_quotes()
        sleep(6)