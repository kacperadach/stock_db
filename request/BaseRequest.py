class BaseRequest():

    def __init__(self):
        pass

    def get_http_method(self):
        return ''

    def get_headers(self):
        return {}

    def get_url(self):
        return ''

    def get_body(self):
        return {}

    @staticmethod
    def parse_response(response):
        pass
