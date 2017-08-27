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

    def get_data(self, request_method):
        data = None
        if request_method.lower() == 'requests':
            try:
                data = json.loads(self.response.text)
            except:
                if hasattr(self.response, 'text') and self.response.text:
                    data = self.response.text
                else:
                    data = ""
        elif request_method.lower() == 'urllib':
            if hasattr(self.response, 'read'):
                data = self.response.read()
            else:
                data = ""
        return data
