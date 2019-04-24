import flask
import json
from bson import json_util

class Response():

    def __init__(self):
        pass

    @staticmethod
    def ok(body):
        response = flask.Response(json.dumps(body, default=json_util.default))
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response

    @staticmethod
    def status(status):
        response = flask.Response(status=status)
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response

