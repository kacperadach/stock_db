from datetime import datetime, timedelta
from pytz import timezone

import flask
from flask import request, abort, jsonify

from Response import Response

from api.data.quote import Quote
from core.data.QuoteRepository import Quote_Repository

quote_view = flask.Blueprint('quote', __name__)

@quote_view.route('/<instrument_type>/<exchange>/<symbol>', methods=['GET'])
def get_quote(instrument_type, exchange, symbol):
    now = datetime.now(timezone('EST'))

    start = now - timedelta(days=60)
    if request.args.get('start') is not None:
        start = datetime.strptime(request.args.get('start'), '%Y-%m-%d')

    end = now
    if request.args.get('end') is not None:
        end = datetime.strptime(request.args.get('end'), '%Y-%m-%d') - timedelta(days=1)

    time_interval = request.args.get('time') or '1d'

    data = Quote_Repository.get_interval(instrument_type, exchange, symbol, start, end, time_interval)

    Quote_Repository.request_quote(instrument_type, exchange, symbol)
    return Response.ok(data)