from copy import deepcopy

from core.StockDbBase import StockDbBase

BASE_URL = 'https://query1.finance.yahoo.com/v1/finance/screener?lang=en-US&region=US&formatted=true&corsDomain=finance.yahoo.com'
REGIONS = ('us', 'ar', 'dk', 'au', 'bh', 'ca', 'cl', 'cz', 'at', 'be', 'br', 'ch', 'cn', 'de', 'eg', 'fi', 'gb', 'hk', 'id', 'il',
 'es', 'fr', 'gr', 'hu', 'ie', 'in', 'jo', 'kr', 'lk', 'it', 'jp', 'kw', 'lu', 'my', 'mx', 'nl', 'nz', 'ph', 'pl', 'qa',
 'se', 'no', 'pe', 'pk', 'pt', 'ru', 'sg', 'sr', 'tf', 'tl', 'tr', 'vn', 'th', 'tn', 'tw', 've', 'za')
BODY = {
    "size":250,
    "offset":0,
    "sortField":"intradaymarketcap",
    "sortType":"DESC",
    "quoteType":"EQUITY",
    "topOperator":"AND",
    "query":{
        "operator":"AND",
        "operands":[
            {
                "operator":"or",
                "operands":[
                    {"operator":"EQ","operands":["region",""]},
                ]
            }
            ]
        },
    "userId":"",
    "userIdType":"guid"
}
OPERAND = {"operator":"EQ","operands":["region",""]}

class YahooFinanceSymbolRequestException(Exception):
    pass

class YahooFinanceSymbolRequest(StockDbBase):

    def __init__(self, regions, offset=0):
        super(YahooFinanceSymbolRequest, self).__init__()
        if not set(regions).issubset(REGIONS):
            raise YahooFinanceSymbolRequestException('Invalid region')
        self.regions = regions
        self.offset = offset

    def get_url(self):
        return BASE_URL

    def get_http_method(self):
        return 'POST'

    def _get_operand(self, region):
        op = deepcopy(OPERAND)
        op['operands'][1] = region
        return op

    def get_body(self):
        body = deepcopy(BODY)
        body['query']['operands'][0]['operands'] = map(self._get_operand, self.regions)
        body['offset'] = self.offset
        return body

    @staticmethod
    def parse_response(response):
        if not isinstance(response, dict) or 'finance' not in response.keys():
            return []
        if not isinstance(response['finance'], dict) or 'result' not in response['finance'].keys():
            return []
        if not isinstance(response['finance']['result'], list) or len(response['finance']['result']) < 1:
            return []
        if not isinstance(response['finance']['result'][0], dict) or 'quotes' not in response['finance']['result'][0].keys():
            return []
        return response['finance']['result'][0]['quotes']
