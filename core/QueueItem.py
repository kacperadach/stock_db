from request.base.ResponseWrapper import ResponseWrapper

MAX_RETRIES = 2
HTTP_METHODS = ('GET', 'POST')

class QueueItem():

    def __init__(self, symbol, url, http_method, callback, body={}, headers={}, metadata={}):
        if not callable(callback):
            raise QueueItemException("callback supplied was not a function")
        if http_method.upper() not in HTTP_METHODS:
            raise QueueItemException('invalid http method: {}'.format(http_method))
        self.symbol = symbol
        self.url = url
        self.http_method = http_method
        self.callback = callback
        self.body = body
        self.headers = headers
        self.metadata = metadata
        self.response = None

    def get_http_method(self):
        return self.http_method

    def get_symbol(self):
        return self.symbol

    def get_url(self):
        return self.url

    def get_body(self):
        return self.body

    def get_headers(self):
        return self.headers

    def add_response(self, response):
        if not isinstance(response, ResponseWrapper):
            raise QueueItemException('response was not a ResponseWrapper instance')
        self.response = response

    def get_metadata(self):
        return self.metadata

    def get_response(self):
        return self.response

class QueueItemException(Exception):
    pass
