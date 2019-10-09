import requests

from discord.webhook import DiscordWebhook
from request.base.ResponseWrapper import ResponseWrapper

URL = "https://query1.finance.yahoo.com/v1/finance/screener"
PAYLOAD = "{\"offset\":0,\"size\":100,\"sortType\":\"DESC\",\"sortField\":\"percentchange\",\"quoteType\":\"ETF\",\"query\":{\"operator\":\"and\",\"operands\":[{\"operator\":\"gt\",\"operands\":[\"eodvolume\",100000]}]},\"userId\":\"\",\"userIdType\":\"guid\"}\r\n"
HEADERS = {
    'content-type': "application/json",
    'cache-control': "no-cache"
}

BODY = {
    "offset": 0,
    "size":100,
    "sortType":"DESC",
    "sortField":"percentchange",
    "quoteType":"ETF",
    "query":{"operator":"and","operands":[{"operator":"gt","operands":["eodvolume",100000]}]},
    "userId":"",
    "userIdType":"guid"
}

class ETF():

    def __init__(self):
        self.task_name = 'ETFs'
        self.discord = DiscordWebhook()

    def _log(self, msg, level='info'):
        pass

    def _get_request_body(self, offset):
        body = BODY
        body['offset'] = offset
        return body

    def get_all_etfs(self):
        found = 0
        size = 100
        etfs = []
        while (found < size):
            response = ResponseWrapper(requests.request("POST", URL, data=str(self._get_request_body(found)), headers=HEADERS))
            if response.status_code == 200:
                data = response.get_data()
                if isinstance(data, dict) and 'finance' in data.keys():
                    try:
                        etfs += list(set(map(lambda x: x['symbol'], data['finance']['result'][0]['quotes'])))
                        size = data['finance']['result'][0]['total']
                        found += data['finance']['result'][0]['count']
                    except (TypeError, KeyError, IndexError) as e:
                        self._log('Error in acquiring ETF symbols: {}'.format(e))
                        break
            else:
                break
        return etfs

if __name__ == "__main__":
    print ETF().get_all_etfs()



