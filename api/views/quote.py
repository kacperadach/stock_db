from datetime import datetime, timedelta
from pytz import timezone

import flask
from flask import request, abort, jsonify

from Response import Response

from api.data.quote import Quote
from core.data.QuoteRepository import Quote_Repository

DELTA_MAP = {
    '15m': 5,
    '1d': 60,
    '1w': 365
}

quote_view = flask.Blueprint('quote', __name__)

@quote_view.route('/<instrument_type>/<exchange>/<symbol>', methods=['GET'])
def get_quote(instrument_type, exchange, symbol):
    now = datetime.now(timezone('EST'))

    time_interval = request.args.get('time') or '1d'

    start = now - timedelta(days=DELTA_MAP[time_interval])
    if request.args.get('start') is not None:
        start = datetime.strptime(request.args.get('start'), '%Y-%m-%d')

    end = now
    if request.args.get('end') is not None:
        end = datetime.strptime(request.args.get('end'), '%Y-%m-%d') - timedelta(days=1)

    data = Quote_Repository.get_interval(instrument_type, exchange, symbol, start, end, time_interval)
    if not data['data'] and time_interval != '1d':
        if time_interval in ('15m', '15M'):
            data = Quote_Repository.get_interval(instrument_type, exchange, symbol, start, end, '1m')

    Quote_Repository.request_quote(instrument_type, exchange, symbol)
    return Response.ok(data)