from copy import deepcopy
from datetime import datetime, time, timedelta
from pytz import timezone

from core.StockDbBase import StockDbBase
from core.market.Market import is_market_open
from core.QueueItem import QueueItem
from request.YahooFinanceSymbolRequest import YahooFinanceSymbolRequest, REGIONS
from db.Finance import Finance_DB

OFFSET_INTERVAL = 250
COLLECTION_NAME = 'symbols'
DESIRED_FIELDS = ('currency', 'exchange', 'exchangeTimezoneShortName', 'fullExchangeName', 'market', 'longName', 'quoteType', 'shortName', 'symbol', 'tradeable')
REQUIRED_FIELDS = ('symbol', 'fullExchangeName')

class ETFSymbolScraper(StockDbBase):

    def __init__(self):
        super(ETFSymbolScraper, self).__init__()
        self.current_day = datetime.now(timezone('EST')).date()
        self.query_dict = {}
        self.counter = 0
        self.regions = deepcopy(REGIONS)
        self.db = Finance_DB

    def get_next_input(self):
        now = datetime.now(timezone('EST'))
        if is_market_open(now):
            return

        if now.date() > self.current_day:
            self.current_day = now.date()
            self.query_dict = {}

        if self.counter >= len(self.regions):
            self.counter = 0

        current_region = self.regions[self.counter]
        self.counter += 1
        if current_region not in self.query_dict.keys():
            symbol_request = YahooFinanceSymbolRequest(regions=[current_region], sectors=[], quote_type='ETF', offset=0)
            self.query_dict[current_region] = {'offset': 0, 'total': None}
            return QueueItem(
                symbol=current_region,
                url=symbol_request.get_url(),
                callback=self.process_data,
                http_method=symbol_request.get_http_method(),
                body=symbol_request.get_body()
            )

        if self.query_dict[current_region]['total'] is None:
            return

        offset = self.query_dict[current_region]['offset']
        if offset <= self.query_dict[current_region]['total']:
            new_offset = offset + OFFSET_INTERVAL
            symbol_request = YahooFinanceSymbolRequest(regions=[current_region], quote_type='ETF', offset=new_offset)
            self.query_dict[current_region]['offset'] = new_offset
            return QueueItem(
                symbol=current_region,
                url=symbol_request.get_url(),
                callback=self.process_data,
                http_method=symbol_request.get_http_method(),
                body=symbol_request.get_body()
            )

    def process_data(self, queue_item):
        data = queue_item.get_response().get_data()
        try:
            data = data['finance']['result'][0]
            total = data['total']
            quotes = data['quotes']
        except Exception:
            self.log('Response dict has incorrect format', level='warn')
            return

        region = queue_item.get_symbol()
        if self.query_dict[region]['total'] is None:
            self.query_dict[region]['total'] = total

        documents = []
        for quote in quotes:
            document = {}
            for key in DESIRED_FIELDS:
                try:
                    document[key] = quote[key]
                except KeyError:
                    if key in REQUIRED_FIELDS:
                        document = {}
                        break
            if document:
                documents.append(document)

        if not documents:
            return

        new_documents = []
        for document in documents:
            if not list(self.db.find(COLLECTION_NAME, {'symbol': document['symbol']}, {}).limit(1)):
                new_documents.append(document)

        if new_documents:
            self.db.insert(COLLECTION_NAME, new_documents)
