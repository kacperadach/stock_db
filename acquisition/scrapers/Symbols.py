from copy import deepcopy
from datetime import datetime, time, timedelta
from pytz import timezone

from core.StockDbBase import StockDbBase
from core.market.Market import is_market_open
from request.YahooFinanceSymbolRequest import YahooFinanceSymbolRequest, REGIONS, SECTORS
from core.QueueItem import QueueItem
from db.Finance import Finance_DB

COLLECTION_NAME = 'symbols'
OFFSET_INTERVAL = 100
REQUIRED_FIELDS = ('symbol', 'fullExchangeName')
DESIRED_FIELDS = ('currency', 'exchange', 'exchangeTimezoneShortName', 'fullExchangeName', 'market', 'longName', 'quoteType', 'shortName', 'symbol', 'tradeable')

class SymbolScraper(StockDbBase):

    def __init__(self):
        super(SymbolScraper, self).__init__()
        self.current_day = datetime.now(timezone('EST')).date()
        self.query_dict = {}
        self.region_counter = 0
        self.sector_counter = 0
        self.regions = deepcopy(REGIONS)
        self.sectors = deepcopy(SECTORS)
        self.db = Finance_DB

    def get_next_input(self):
        now = datetime.now(timezone('EST'))
        if is_market_open(now):
            return

        if now.date() > self.current_day:
            self.current_day = now.date()
            self.query_dict = {}

        if self.sector_counter >= len(self.sectors):
            self.sector_counter = 0
            self.region_counter += 1

        if self.region_counter >= len(self.regions):
            self.region_counter = 0

        current_region = self.regions[self.region_counter]
        current_sector = self.sectors[self.sector_counter]
        if current_region not in self.query_dict.keys() or current_sector not in self.query_dict[current_region].keys():
            symbol_request = YahooFinanceSymbolRequest(regions=[current_region], sectors=[current_sector], quote_type='EQUITY', offset=0, size=OFFSET_INTERVAL)
            if current_region not in self.query_dict.keys():
                self.query_dict[current_region] = {current_sector: {'offset': 0, 'total': None}}
            else:
                self.query_dict[current_region][current_sector] =  {'offset': 0, 'total': None}
            self.sector_counter += 1
            return QueueItem(
                symbol=current_region,
                url=symbol_request.get_url(),
                callback=self.process_data,
                http_method=symbol_request.get_http_method(),
                body=symbol_request.get_body(),
                metadata={'region': current_region, 'sector': current_sector}
            )

        if self.query_dict[current_region][current_sector]['total'] is None:
            self.sector_counter += 1
            return

        offset = self.query_dict[current_region][current_sector]['offset']
        if offset <= self.query_dict[current_region][current_sector]['total']:
            new_offset = offset + OFFSET_INTERVAL
            symbol_request = YahooFinanceSymbolRequest(regions=[current_region], sectors=[current_sector], quote_type='EQUITY', offset=new_offset, size=OFFSET_INTERVAL)
            self.query_dict[current_region][current_sector]['offset'] = new_offset
            self.sector_counter += 1
            return QueueItem(
                symbol=current_region,
                url=symbol_request.get_url(),
                callback=self.process_data,
                http_method=symbol_request.get_http_method(),
                body=symbol_request.get_body(),
                metadata={'region': current_region, 'sector': current_sector}
            )
        self.sector_counter += 1

    def process_data(self, queue_item):
        data = queue_item.get_response().get_data()
        try:
            data = data['finance']['result'][0]
            total = data['total']
            quotes = data['quotes']
        except Exception:
            self.log('Response dict has incorrect format', level='warn')
            return

        region = queue_item.get_metadata()['region']
        sector = queue_item.get_metadata()['sector']
        if self.query_dict[region][sector]['total'] is None:
            self.query_dict[region][sector]['total'] = total

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
                document['region'] = region
                document['sector'] = sector
                documents.append(document)

        if not documents:
            return

        existing_symbols = self.db.distinct(COLLECTION_NAME, 'symbol', {'region': region})
        new_documents = filter(lambda x: x['symbol'] not in existing_symbols, documents)

        if new_documents:
            self.db.insert(COLLECTION_NAME, new_documents)
