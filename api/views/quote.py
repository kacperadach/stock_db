from datetime import datetime, timedelta
from pytz import timezone

import flask
from flask import request, abort, jsonify

from api.data.quote import Quote

quote_view = flask.Blueprint('quote', __name__)

@quote_view.route('/<instrument_type>/<symbol>', methods=['GET'])
def get_quote(instrument_type, symbol):
    now = datetime.now(timezone('EST'))
    start = request.args.get('start') or (now - timedelta(days=30))
    end = request.args.get('end') or now
    time_interval = request.args.get('time_interval') or '1d'
    return Quote().get(instrument_type, symbol, start, end, time_interval)