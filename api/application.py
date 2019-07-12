import os
import json
from bson import json_util

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
    print chart
    symbol = decrypt_unique_id(chart['uid'])
    start = None if 'start' not in chart.keys() else chart['start']
    end = None if 'start' not in chart.keys() else chart['end']
    print start
    emit('chart', json.dumps(get_quote(symbol['instrument_type'], symbol['exchange'], symbol['symbol'], start=start, end=end), default=json_util.default))

@socketio.on('message')
def handle_message(message):
    print('received message: ' + message)
    send({'data': 'test'}, json=True, callback=ack, broadcast=False)
    emit('event', {'data': 'test'})
    #emit for named messages

# app.register_blueprint(quote_view, url_prefix="/quote")
# app.register_blueprint(symbols_view, url_prefix="/symbols")

dir_path = os.path.dirname(os.path.realpath(__file__))

# Serve React App
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve(path):
    print path
    if path != "" and os.path.exists(os.path.join(dir_path,'react_app/build', path)):
        return send_from_directory('react_app/build', path)
    else:
        return send_from_directory('react_app/build', 'index.html')

def ack():
    print 'message was received!'

def run():
    socketio.run(app, debug=True)
    # app.run(use_reloader=True, port=5000, threaded=False)

if __name__ == '__main__':
    run()