from copy import deepcopy
from datetime import datetime
from pytz import timezone

from core.BaseScraper import BaseScraper
from core.QueueItem import QueueItem
from core.data.QuoteRepository import Quote_Repository
from db.Finance import Finance_DB
from request.MarketWatchRequest import MarketWatchRequest
from request.MarketWatchRequestIndicators import MarketWatchRequestIndicators

SYMBOL_COLLECTION = 'market_watch_symbols'

class MarketWatchHistoricalScraper(BaseScraper):

    def __init__(self, indicators=None):
        super(MarketWatchHistoricalScraper, self).__init__()
        self.today = datetime.now(timezone('EST')).date()
        self.db = Finance_DB
        self.scrape_dict = {}
        self.symbols = []
        self.symbols_cursor = self.get_symbols()
        self.quote_repository = Quote_Repository
        if indicators is None:
            indicators = MarketWatchRequestIndicators(use_default=True)
        self.indicators = indicators

    def get_symbols(self):
        return self.db.find(SYMBOL_COLLECTION, {}, {'symbol': 1, 'instrument_type': 1, 'exchange': 1, 'country': 1, 'country_code': 1})

    def get_next_input(self):
        now = datetime.now(timezone('EST'))

        if now.date() != self.today:
            self.today = now.date()
            self.scrape_dict = {}
            self.symbols_cursor = self.get_symbols()

        for symbol in self.symbols_cursor:
            unique_id = self.get_unique_id(symbol['symbol'], symbol['instrument_type'], symbol['exchange'])
            if unique_id not in self.scrape_dict.keys():
                self.scrape_dict[unique_id] = True
                mwr = MarketWatchRequest(symbol=symbol, step_interval='1d', instrument_type=symbol['instrument_type'], indicators=self.indicators)
                return QueueItem(url=mwr.get_url(), http_method=mwr.get_http_method(), headers=mwr.get_headers(), callback=self.process_data, metadata={'symbol': symbol, 'type': 'historical', 'indicators': self.indicators})

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

        self.quote_repository.insert(data, metadata)
