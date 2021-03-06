import json

class ResponseWrapper():

    def __init__(self, response):
        self.response = response
        self.status_code = self._get_response_code(response)

    def _get_response_code(self, response):
        if hasattr(response, 'status_code'):
            return response.status_code
        elif hasattr(response, 'code'):
            return response.code
        return 404

    def is_successful(self):
        return self.status_code == 200

    def get_data(self):
        data = {}
        if self.response is not None and hasattr(self.response, 'text'):
            try:
                data = json.loads(self.response.text)
            except:
                if self.response.text:
                    data = self.response.text
                else:
                    data = ""
        return data

    def get_response_headers(self):
        return dict(self.response.headers._store)

    def get_response_cookies(self):
        return self.response.cookies.get_dict()
