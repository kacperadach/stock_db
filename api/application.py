from flask import Flask, request, abort, jsonify

from views.quote import quote_view

application = Flask(__name__)

application.register_blueprint(quote_view, url_prefix="/quote")

@application.route("/")
def hello():
    return "Hello World!"

if __name__ == '__main__':
    application.run()