import os
import json
from bson import json_util
from datetime import datetime, timedelta

# for socketio
from gevent import monkey

from api.views.meta_data import fetch_metadata
from core.data.FuturesRepository import FuturesRepository
from core.data.uid import decrypt_unique_id, encrypt_unique_id

monkey.patch_all()


from flask import Flask, send_from_directory, render_template
from flask_socketio import SocketIO, send, emit

from views.quote import get_quote, get_live_futures_quotes
from views.scraper import get_stats

from core.data.SymbolRepository import Symbol_Repository

app = Flask(__name__, static_folder='react_app/build')

app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, async_mode='gevent')

@socketio.on('scraper_stats')
def get_scraper_stats(d):
    emit('scraper_stats', json.dumps(get_stats(), default=json_util.default))

@socketio.on('live_futures')
def get_live_futures(meta):
    print meta
    # emit('live_futures', get_live_futures_quotes(meta['offset'], meta['limit']))
    emit('live_futures', json.dumps(get_live_futures_quotes(), default=json_util.default))


@socketio.on('symbols')
def search_symbols(searchTerm):
    emit('symbols', {'term': searchTerm, 'results': Symbol_Repository.search(searchTerm)})

@socketio.on('chart')
def get_chart(chart):
    symbol = decrypt_unique_id(chart['uid'])
    start = None if 'start' not in chart.keys() else chart['start']
    end = None if 'start' not in chart.keys() else chart['end']
    emit('chart', json.dumps(get_quote(symbol['instrument_type'], symbol['exchange'], symbol['symbol'], start=start, end=end), default=json_util.default))

@socketio.on('metadata')
def get_metadata(chart):
    data = fetch_metadata(chart['uid'])
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

def run():
    socketio.run(app, debug=True)
    # app.run(use_reloader=True, port=5000, threaded=False)

if __name__ == '__main__':
    get_metadata({'uid': 'U0kwMCAvWE5ZTSAvVVMgIC9mdXR1cmVzICAg'})
    # run()

