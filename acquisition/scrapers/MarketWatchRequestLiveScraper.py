from copy import deepcopy
from datetime import datetime
import time

from core.StockDbBase import StockDbBase
from core.QueueItem import QueueItem
from core.market.Market import is_market_open
from db.Finance import Finance_DB
from core.data.QuoteRepository import Quote_Repository
from request.MarketWatchRequest import MarketWatchRequest

from acquisition.scrapers.MarketWatchLiveScraper import MarketWatchLiveScraper

REQUEST_COLLECTION = 'market_watch_request'

LIMIT = 100

REQUEST_FIELDS = {'instrument_type': 1, 'exchange': 1, 'symbol': 1}
FIELDS = {'symbol': 1, 'instrument_type': 1, 'exchange': 1, 'country': 1, 'country_code': 1}

LIVE_SCRAPE_PERIOD_SEC = 60

""" Looks at REQUEST_COLLECTION and scrapes those symbols """
class MarketWatchRequestLiveScraper(MarketWatchLiveScraper):
    def get_symbols(self):
        requests = self.db.find(REQUEST_COLLECTION, {}, REQUEST_FIELDS).sort('timestamp', -1).limit(LIMIT)
        requests = {request['instrument_type'] + request['exchange'] + request['symbol']: request for request in requests}
        cursor = []
        if requests.values():
            cursor = self.db.find(self.MARKET_WATCH_SYMBOL_COLLECTION, {'$or': requests.values()}, FIELDS)
        return cursor

    def get_next_input(self):
        now = datetime.now()

        if self.symbols_cursor is None:
            self.symbols_cursor = list(self.get_symbols())

        symbols = list(self.symbols_cursor)

        for symbol in symbols:
            unique_id = str(symbol['symbol']) + str(symbol['instrument_type']) +  str(symbol['exchange'])
            if unique_id not in self.scrape_dict.keys():
                self.scrape_dict[unique_id] = {'live': now, 'historical': None}
                mwr = MarketWatchRequest(symbol=symbol, step_interval='1m', instrument_type=symbol['instrument_type'])
                return QueueItem(url=mwr.get_url(), http_method=mwr.get_http_method(), headers=mwr.get_headers(), callback=self.process_data, metadata={'symbol': symbol, 'time_interval': '1m', 'indicators': mwr.get_indicators()})
            elif self.scrape_dict[unique_id]['historical'] is None:
                self.scrape_dict[unique_id]['historical'] = now
                mwr = MarketWatchRequest(symbol=symbol, step_interval='1d', instrument_type=symbol['instrument_type'])
                return QueueItem(url=mwr.get_url(), http_method=mwr.get_http_method(), headers=mwr.get_headers(), callback=self.process_data, metadata={'symbol': symbol, 'time_interval': '1d', 'indicators': mwr.get_indicators()})
            elif (now - self.scrape_dict[unique_id]['live']).total_seconds() >= LIVE_SCRAPE_PERIOD_SEC:
                self.scrape_dict[unique_id]['live'] = now
                mwr = MarketWatchRequest(symbol=symbol, step_interval='1m', instrument_type=symbol['instrument_type'])
                return QueueItem(url=mwr.get_url(), http_method=mwr.get_http_method(), headers=mwr.get_headers(), callback=self.process_data, metadata={'symbol': symbol, 'time_interval': '1m', 'indicators': mwr.get_indicators()})
            elif (now - self.scrape_dict[unique_id]['historical']).total_seconds() >= LIVE_SCRAPE_PERIOD_SEC:
                self.scrape_dict[unique_id]['historical'] = now
                mwr = MarketWatchRequest(symbol=symbol, step_interval='1d', instrument_type=symbol['instrument_type'])
                return QueueItem(url=mwr.get_url(), http_method=mwr.get_http_method(), headers=mwr.get_headers(), callback=self.process_data, metadata={'symbol': symbol, 'time_interval': '1d', 'indicators': mwr.get_indicators()})
        self.symbols_cursor = self.get_symbols()

    def process_data(self, queue_item):
        meta = queue_item.get_metadata()
        Quote_Repository.delete_request_quote(meta['symbol']['instrument_type'], meta['symbol']['exchange'], meta['symbol']['symbol'])
        super(MarketWatchRequestLiveScraper, self).process_data(queue_item=queue_item)