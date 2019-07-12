from datetime import datetime, timedelta
from pytz import timezone

import flask
from flask import request, abort, jsonify

from Response import Response

from api.data.quote import Quote
from core.data.SymbolRepository import Symbol_Repository
from Response import Response

symbols_view = flask.Blueprint('symbols', __name__)

@symbols_view.route('search/<symbol>', methods=['GET'])
def search_symbols(symbol):
    search_results = Symbol_Repository.search(symbol)
    return Response.ok(search_results)

@symbols_view.route('/<instrument_type>/<exchange>/<symbol>', methods=['GET'])
def get_symbol(instrument_type, exchange, symbol):
    symbol = Symbol_Repository.get(symbol, instrument_type=instrument_type, exchange=exchange)
    if symbol:
        return Response.ok(symbol)
    else:
        return Response.status(404)
