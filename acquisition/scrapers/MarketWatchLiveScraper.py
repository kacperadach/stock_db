from copy import deepcopy
from datetime import datetime
from pytz import timezone
import time

from core.BaseScraper import BaseScraper
from core.QueueItem import QueueItem
from core.market.Market import is_market_open
from db.Finance import Finance_DB
from core.data.QuoteRepository import Quote_Repository
from request.MarketWatchRequest import MarketWatchRequest
from request.MarketWatchRequestIndicators import MarketWatchRequestIndicators

SYMBOLS_COLLECTION = 'symbols'
LIVE_SCRAPE_PERIOD_SEC = 900

class MarketWatchLiveScraper(BaseScraper):
    MARKET_WATCH_SYMBOL_COLLECTION = 'market_watch_symbols'

    def __init__(self, indicators=None):
        super(MarketWatchLiveScraper, self).__init__()
        self.today = datetime.now(timezone('EST')).date()
        self.db = Finance_DB
        self.quote_repository = Quote_Repository
        self.scrape_dict = {}
        self.ignored_symbols = {}
        if indicators is None:
            indicators = MarketWatchRequestIndicators(use_default=True)
        self.indicators = indicators
        self.symbols_cursor = self.get_symbols()

    # def get_symbols(self):
    #     return self.db.find(self.MARKET_WATCH_SYMBOL_COLLECTION, {'symbol': 'AAPL', 'country': 'united-states'}, {'symbol': 1, 'instrument_type': 1, 'exchange': 1, 'country': 1, 'country_code': 1})

    def get_next_input(self):
        now = datetime.now(timezone('EST'))

        if now.date() != self.today:
            self.today = now.date()
            self.scrape_dict = {}
            self.ignored_symbols = {}
            self.symbols_cursor = self.get_symbols()

        for symbol in self.symbols_cursor:
            unique_id = self.get_unique_id(symbol['symbol'], symbol['instrument_type'], symbol['exchange'])
            if unique_id in self.ignored_symbols:
                continue
            if unique_id not in self.scrape_dict.keys():
                self.scrape_dict[unique_id] = now
                mwr = MarketWatchRequest(symbol=symbol, step_interval='1m', instrument_type=symbol['instrument_type'], indicators=self.indicators)
                return QueueItem(url=mwr.get_url(), http_method=mwr.get_http_method(), headers=mwr.get_headers(), callback=self.process_data, metadata={'symbol': symbol, 'time_interval': '1m', 'indicators': self.indicators})
            elif (now - self.scrape_dict[unique_id]).total_seconds() >= LIVE_SCRAPE_PERIOD_SEC:
                self.scrape_dict[unique_id] = now
                mwr = MarketWatchRequest(symbol=symbol, step_interval='1m', instrument_type=symbol['instrument_type'])
                return QueueItem(url=mwr.get_url(), http_method=mwr.get_http_method(), headers=mwr.get_headers(), callback=self.process_data, metadata={'symbol': symbol, 'time_interval': '1m'})

        self.get_symbols()

    def get_unique_id(self, symbol, instrument_type, exchange):
        return str(symbol) + str(instrument_type) +  str(exchange)

    def process_data(self, queue_item):
        data = MarketWatchRequest.parse_response(queue_item.get_response().get_data())

        if not data:
            symbol = queue_item.get_metadata()['symbol']
            self.ignored_symbols[self.get_unique_id(symbol['symbol'], symbol['instrument_type'], symbol['exchange'])] = True
            return

        metadata = queue_item.get_metadata()
        if metadata['symbol']['symbol'] != data['symbol']:
            self.log('symbol from request does not match response', level='warn')
            return

        if 'time_interval' not in metadata.keys():
            raise RuntimeError('need time_interval in meta')

        self.quote_repository.insert(data, metadata)
