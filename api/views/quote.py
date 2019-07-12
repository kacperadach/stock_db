from datetime import datetime, timedelta
from pytz import timezone

from Response import Response

from core.data.QuoteRepository import Quote_Repository

DELTA_MAP = {
    '15m': 5,
    '1d': 60,
    '1w': 365
}

def get_quote(instrument_type, exchange, symbol, time_interval='1d', start=None, end=None):
    now = datetime.now(timezone('EST'))

    start_date = now - timedelta(days=DELTA_MAP[time_interval])
    if start is not None:
        start_date = datetime.strptime(start, '%Y-%m-%d')

    end_date = now
    if end is not None:
        end_date = datetime.strptime(end, '%Y-%m-%d') - timedelta(days=1)

    data = Quote_Repository.get_interval(instrument_type, exchange, symbol, start_date, end_date, time_interval)
    if not data['data'] and time_interval != '1d':
        if time_interval in ('15m', '15M'):
            data = Quote_Repository.get_interval(instrument_type, exchange, symbol, start_date, end_date, '1m')

    Quote_Repository.request_quote(instrument_type, exchange, symbol)
    return data