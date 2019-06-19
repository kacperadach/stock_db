import os
from flask import Flask, send_from_directory

# from views.quote import quote_view
# from views.symbols import symbols_view
#
# app = Flask(__name__, static_folder='react_app/build')
#
# app.register_blueprint(quote_view, url_prefix="/quote")
# app.register_blueprint(symbols_view, url_prefix="/symbols")
#
# # Serve React App
# @app.route('/', defaults={'path': ''})
# @app.route('/<path:path>')
# def serve(path):
#     print path
#     if path != "" and os.path.exists("react_app/build/" + path):
#         return send_from_directory('react_app/build', path)
#     else:
#         return send_from_directory('react_app/build', 'index.html')

app = Flask(__name__)

@app.route("/")
def hello():
    return "<h1 style='color:blue'>Hello There!</h1>"


def run():
    app.run(host='0.0.0.0', use_reloader=True, port=5000, threaded=False)

if __name__ == '__main__':
    run()