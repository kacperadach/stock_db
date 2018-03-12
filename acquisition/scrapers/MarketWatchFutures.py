from datetime import datetime, timedelta

from core.StockDbBase import StockDbBase
from core.QueueItem import QueueItem
from db.Finance import Finance_DB

from request.MarketWatchFutures import MarketWatchFutures
from request.MarketWatchForexRequest import MarketWatchForexRequest

FUTURES_SYMBOLS_COLLECTION = 'futures_symbols'
FUTURES_COLLECTION = 'futures'
REAL_TIME_SCRAPE_MINUTE_FREQ = 5

class MarketWatchFuturesScraper(StockDbBase):

    def __init__(self):
        super(MarketWatchFuturesScraper, self).__init__()
        self.db = Finance_DB
        self.last_fetched_symbols = None
        self.scrape_dict = {}
        self._reset()

    def _reset(self):
        self.counter = 0
        self.futures_symbols = map(lambda x: x['symbol'], list(self.db.find(FUTURES_SYMBOLS_COLLECTION, {}, {'symbol': 1})))

    def get_next_input(self):
        now = datetime.now()

        if self.last_fetched_symbols is None or self.last_fetched_symbols < datetime.now().date():
            self.last_fetched_symbols = datetime.now().date()
            self.scrape_dict = {}
            mwf = MarketWatchFutures()
            return QueueItem(
                symbol='futures_symbols',
                url=mwf.get_url(),
                http_method=mwf.get_http_method(),
                callback=self.process_data
            )

        if len(self.futures_symbols) == 0:
            self._reset()
            return

        if self.counter >= len(self.futures_symbols):
            self._reset()

        current_symbol = self.futures_symbols[self.counter]
        self.counter += 1
        if current_symbol not in self.scrape_dict.keys():
            self.scrape_dict[current_symbol] = {'real_time': None, 'historical': False}

        if self.scrape_dict[current_symbol]['real_time'] is None or self.scrape_dict[current_symbol]['real_time'] < now - timedelta(minutes=REAL_TIME_SCRAPE_MINUTE_FREQ):
            self.scrape_dict[current_symbol]['real_time'] = now
            mwfr = MarketWatchForexRequest(current_symbol, '1m')
            return QueueItem(
                symbol=current_symbol,
                url = mwfr.get_url(),
                http_method=mwfr.get_http_method(),
                callback=self.process_data,
                headers=mwfr.get_headers()
            )
        elif self.scrape_dict[current_symbol]['historical'] is False:
            self.scrape_dict[current_symbol]['historical'] = True
            mwfr = MarketWatchForexRequest(current_symbol, '1d')
            return QueueItem(
                symbol=current_symbol,
                url=mwfr.get_url(),
                http_method=mwfr.get_http_method(),
                callback=self.process_data,
                headers=mwfr.get_headers()
            )

    def process_data(self, queue_item):
        if queue_item.get_symbol() == 'futures_symbols':
            documents = MarketWatchFutures.parse_response(queue_item.get_response().get_data())
            existing_symbols = map(lambda x: x['symbol'], list(self.db.find(FUTURES_SYMBOLS_COLLECTION, {}, {'symbol': 1})))
            new_documents = []
            for document in documents:
                if document['symbol'] not in existing_symbols:
                    new_documents.append(document)
            if new_documents:
                self.db.insert(FUTURES_SYMBOLS_COLLECTION, new_documents)
            return

        a = 1

