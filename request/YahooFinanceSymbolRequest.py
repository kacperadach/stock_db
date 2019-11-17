from copy import deepcopy

from core.StockDbBase import StockDbBase

SIZE_INTERVAL = 100
BASE_URL = 'https://query1.finance.yahoo.com/v1/finance/screener?lang=en-US&region=US&formatted=true&corsDomain=finance.yahoo.com'
REGIONS = ('us', 'ar', 'dk', 'au', 'bh', 'ca', 'cl', 'cz', 'at', 'be', 'br', 'ch', 'cn', 'de', 'eg', 'fi', 'gb', 'hk', 'id', 'il',
 'es', 'fr', 'gr', 'hu', 'ie', 'in', 'jo', 'kr', 'lk', 'it', 'jp', 'kw', 'lu', 'my', 'mx', 'nl', 'nz', 'ph', 'pl', 'qa',
 'se', 'no', 'pe', 'pk', 'pt', 'ru', 'sg', 'sr', 'tf', 'tl', 'tr', 'vn', 'th', 'tn', 'tw', 've', 'za')
BODY = {
    "size":0,
    "offset":0,
    "sortField":"",
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
            },
            {
                "operator":"or",
                "operands":[
                    {"operator":"EQ","operands":["sector",""]},
                ]
            }
            ]
        },
    "userId":"",
    "userIdType":"guid"
}
REGION_OPERAND = {"operator":"EQ","operands":["region",""]}
SECTOR_OPERAND = {"operator":"EQ","operands":["sector",""]}
QUOTE_TYPES = ("EQUITY", "ETF")
SORT_FIELD = {
    "EQUITY": "intradaymarketcap",
    "ETF": "fundnetassets"
}
SECTORS = (
    'Basic Materials',
    'Conglomerates',
    'Financial',
    'Industrial Goods',
    'Technology',
    'Consumer Goods',
    'Healthcare',
    'Services',
    'Utilities'
)

class YahooFinanceSymbolRequestException(Exception):
    pass

class YahooFinanceSymbolRequest(StockDbBase):

    def __init__(self, regions, sectors, quote_type, offset=0, size=SIZE_INTERVAL):
        super(YahooFinanceSymbolRequest, self).__init__()
        if not set(regions).issubset(REGIONS):
            raise YahooFinanceSymbolRequestException('Invalid region')
        if not set(sectors).issubset(SECTORS):
            raise YahooFinanceSymbolRequestException('Invalid sector')
        if quote_type not in QUOTE_TYPES:
            raise YahooFinanceSymbolRequestException('Invalid quote type')
        if size > 250:
            raise YahooFinanceSymbolRequestException('Size Interval too large')
        self.regions = regions
        self.sectors = sectors
        self.quote_type = quote_type
        self.offset = offset
        self.size = size

    def get_url(self):
        return BASE_URL

    def get_http_method(self):
        return 'POST'

    def _get_region_operand(self, region):
        op = deepcopy(REGION_OPERAND)
        op['operands'][1] = region
        return op

    def _get_sector_operand(self, sector):
        op = deepcopy(SECTOR_OPERAND)
        op['operands'][1] = sector
        return op

    def get_body(self):
        body = deepcopy(BODY)
        body['query']['operands'][0]['operands'] = list(map(self._get_region_operand, self.regions))
        body['query']['operands'][1]['operands'] = list(map(self._get_sector_operand, self.sectors))
        body['offset'] = self.offset
        body['quoteType'] = self.quote_type
        body['sortField'] = SORT_FIELD[self.quote_type]
        body['size'] = self.size
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
