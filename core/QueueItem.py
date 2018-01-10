from request.base.ResponseWrapper import ResponseWrapper

MAX_RETRIES = 2

class QueueItem():

    def __init__(self, symbol, url, callback, metadata={}):
        if not callable(callback):
            raise QueueItemException("callback supplied was not a function")
        self.symbol = symbol
        self.url = url
        self.callback = callback
        self.metadata = metadata
        self.response = None

    def get_symbol(self):
        return self.symbol

    def get_url(self):
        return self.url

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
