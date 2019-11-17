from copy import deepcopy
from datetime import datetime
from pytz import timezone

from core.StockDbBase import StockDbBase
from core.QueueItem import QueueItem
from db.Finance import Finance_DB
from request.MarketWatchRequest import MarketWatchRequest

SYMBOL_COLLECTION = 'market_watch_symbols'
LIVE_SCRAPE_PERIOD_SEC = 900

class MarketWatchScraper(StockDbBase):

    def __init__(self):
        super(MarketWatchScraper, self).__init__()
        self.today = datetime.now(timezone('EST')).date()
        self.db = Finance_DB
        self.scrape_dict = {}
        self.symbols = []
        self.get_symbols()

    def get_symbols(self):
        self.symbols_cursor = self.db.find(SYMBOL_COLLECTION, {'instrument_type': 'stocks'}, {'symbol': 1, 'instrument_type': 1, 'Exchange': 1, 'Country': 1, 'countryCode': 1})

    def get_next_input(self):
        now = datetime.now(timezone('EST'))

        if now.date() != self.today:
            self.today = now.date()
            self.scrape_dict = {}

        for symbol in self.symbols_cursor:
            unique_id = self.get_unique_id(symbol['symbol'], symbol['instrument_type'], symbol['Exchange'])
            if unique_id not in self.scrape_dict.keys():
                self.scrape_dict[unique_id] = {'live': now, 'historical': False}
                mwr = MarketWatchRequest(symbol=self.get_symbol(symbol), step_interval='1m')
                return QueueItem(url=mwr.get_url(), http_method=mwr.get_http_method(), headers=mwr.get_headers(), callback=self.process_data, metadata={'symbol': symbol, 'type': 'live'})
            elif (now - self.scrape_dict[unique_id]['live']).total_seconds() >= LIVE_SCRAPE_PERIOD_SEC:
                self.scrape_dict[unique_id]['live'] = now
                mwr = MarketWatchRequest(symbol=self.get_symbol(symbol), step_interval='1m')
                return QueueItem(url=mwr.get_url(), http_method=mwr.get_http_method(), headers=mwr.get_headers(), callback=self.process_data, metadata={'symbol': symbol, 'type': 'live'})
            elif self.scrape_dict[unique_id]['historical'] is False:
                self.scrape_dict[unique_id]['historical'] = True
                mwr = MarketWatchRequest(symbol=self.get_symbol(symbol), step_interval='1d')
                return QueueItem(url=mwr.get_url(), http_method=mwr.get_http_method(), headers=mwr.get_headers(), callback=self.process_data, metadata={'symbol': symbol, 'type': 'historical'})

        self.get_symbols()

    def get_symbol(self, symbol):
        instrument_type = symbol['instrument_type']
        if instrument_type.lower() == 'rates':
            if symbol['Country'] == 'Money Rates':
                return 'INTERSTATE/{}/{}/{}'.format(symbol['countryCode'], symbol['Exchange'], symbol['symbol'])
            else:
                return 'LOANRATE/{}/{}/{}'.format(symbol['countryCode'], symbol['Exchange'], symbol['symbol'])
        elif instrument_type.lower() == 'funds':
            return 'FUND/{}/{}/{}'.format(symbol['countryCode'], symbol['Exchange'], symbol['symbol'])
        elif instrument_type.lower() == 'bonds':
            return 'BOND/{}/{}/{}'.format(symbol['countryCode'], symbol['Exchange'], symbol['symbol'])
        elif instrument_type.lower() == 'benchmarks':
            return 'STOCK/{}/{}/{}'.format(symbol['countryCode'], symbol['Exchange'], symbol['symbol'])
        elif instrument_type.lower() == 'american-depository-receipt-stocks':
            return 'STOCK/{}/{}/{}'.format(symbol['countryCode'], symbol['Exchange'], symbol['symbol'])
        elif instrument_type.lower() == 'exchange-traded-notes':
            return 'STOCK/{}/{}/{}'.format(symbol['countryCode'], symbol['Exchange'], symbol['symbol'])
        elif instrument_type.lower() == 'warrants':
            return 'STOCK/{}/{}/{}'.format(symbol['countryCode'], symbol['Exchange'], symbol['symbol'])
        elif instrument_type.lower() == 'stocks':
            return 'STOCK/{}/{}/{}'.format(symbol['countryCode'], symbol['Exchange'], symbol['symbol'])
        elif instrument_type.lower() == 'indexes':
            return 'INDEX/{}/{}/{}'.format(symbol['countryCode'], symbol['Exchange'], symbol['symbol'])
        elif instrument_type.lower() == 'exchange-traded-funds':
            return 'FUND/{}/{}/{}'.format(symbol['countryCode'], symbol['Exchange'], symbol['symbol'])
        elif instrument_type.lower() == 'currencies':
            return 'CURRENCY/{}/{}/{}'.format(symbol['countryCode'], symbol['Exchange'], symbol['symbol'])
        elif instrument_type.lower() == 'crypto-currencies':
            return 'CRYPTOCURRENCY/{}/{}/{}'.format(symbol['countryCode'], symbol['Exchange'], symbol['symbol'])
        elif instrument_type.lower() == 'real-estate-investment-trusts':
            return 'STOCK/{}/{}/{}'.format(symbol['countryCode'], symbol['Exchange'], symbol['symbol'])
        else:
            self.log(instrument_type)
            raise AssertionError('This shouldnt happen!')

    # for the purpose of storing in scrape_dict
    def get_unique_id(self, symbol, instrument_type, exchange):
        return str(symbol) + str(instrument_type) + str(exchange)

    def get_collection_name(self, instrument_type):
        return 'market_watch_' + str(instrument_type)

    def process_data(self, queue_item):
        data = MarketWatchRequest.parse_response(queue_item.get_response().get_data())

        if not data:
            return

        metadata = queue_item.get_metadata()
        if metadata['symbol']['symbol'] != data['symbol']:
            self.log('symbol from request does not match response', level='warn')
            return

        data['exchange'] = metadata['symbol']['Exchange']

        days = {}
        for d in data['data']:
            if d['datetime'].date() not in days.keys():
                days[d['datetime'].date()] = [d]
            else:
                days[d['datetime'].date()] += [d]

        del data['data']
        collection_name = self.get_collection_name(metadata['symbol']['instrument_type'])
        documents = []
        for day, d in days.items():
            trading_date = datetime.combine(day, datetime.min.time())
            new_document = deepcopy(data)
            new_document['data'] = d
            new_document['trading_date'] = trading_date
            documents.append(new_document)

        trading_days = list(map(lambda x: x['trading_date'], documents))
        existing_documents = list(self.db.find(collection_name, {'trading_date': {'$in': trading_days}, 'symbol': metadata['symbol']['symbol'], 'exchange': metadata['symbol']['Exchange'], 'time_interval': data['time_interval']}, {'trading_date': 1, 'data': 1}))

        new_documents = list(filter(lambda x: x['trading_date'] not in map(lambda x: x['trading_date'], existing_documents), documents))

        if new_documents:
            try:
                self.db.insert(collection_name, new_documents)
            except Exception:
                pass
