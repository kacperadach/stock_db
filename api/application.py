import os
import json
from bson import json_util
from datetime import datetime, timedelta

# for socketio
from gevent import monkey

from core.data.uid import decrypt_unique_id

monkey.patch_all()


from flask import Flask, send_from_directory, render_template
from flask_socketio import SocketIO, send, emit

from views.quote import get_quote
from views.symbols import symbols_view

from core.data.SymbolRepository import Symbol_Repository
from views.Response import Response

app = Flask(__name__, static_folder='react_app/build')

app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, async_mode='gevent')

@socketio.on('symbols')
def search_symbols(searchTerm):
    print searchTerm
    emit('symbols', {'term': searchTerm, 'results': Symbol_Repository.search(searchTerm)})

@socketio.on('chart')
def get_chart(chart):
    symbol = decrypt_unique_id(chart['uid'])
    start = None if 'start' not in chart.keys() else chart['start']
    end = None if 'start' not in chart.keys() else chart['end']
    emit('chart', json.dumps(get_quote(symbol['instrument_type'], symbol['exchange'], symbol['symbol'], start=start, end=end), default=json_util.default))

@socketio.on('metadata')
def get_metadata(chart):
    symbol = decrypt_unique_id(chart['uid'])
    print symbol

    instrument_type = symbol['instrument_type']
    exchange = symbol['exchange']
    sym = symbol['symbol']

    data = Symbol_Repository.get(sym, exchange, instrument_type)

    today = datetime.now().date()

    prev_days = get_quote(instrument_type, exchange, sym, time_interval='1d', start=today - timedelta(days=6), end=today + timedelta(days=1), limit=2)
    if len(prev_days['data']) == 2:
        previous = prev_days['data'][0]
        most_recent = prev_days['data'][1]
        # if datetime.strptime(most_recent['date'].split()[0], '%Y-%m-%d').date() == datetime.today().date():
        point_diff = most_recent['close'] - previous['close']
        percentage_diff = point_diff / previous['close'] * 100

        point_diff = '{:.2f}'.format(point_diff)
        percentage_diff = '{:.2f}'.format(percentage_diff)

        data['most_recent_day'] = most_recent['date']
        data['point_diff'] = point_diff
        data['percentage_diff'] = percentage_diff
        data['close'] = most_recent['close']

    print data
    emit('metadata', data)

dir_path = os.path.dirname(os.path.realpath(__file__))
#
# Serve React App
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve(path):
    print path
    if path != "" and os.path.exists(os.path.join(dir_path,'react_app/build', path)):
        return send_from_directory('react_app/build', path)
    else:
        return send_from_directory('react_app/build', 'index.html')

# def ack():
#     print 'message was received!'
#
def run():
    socketio.run(app, debug=True)
    # app.run(use_reloader=True, port=5000, threaded=False)

if __name__ == '__main__':
    # get_metadata({'uid': 'SFVCICAvWFdBUiAvUEwgIC9zdG9ja3MgICAg'})
    run()
