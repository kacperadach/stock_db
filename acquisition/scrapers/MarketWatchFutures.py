from datetime import datetime, timedelta
from pytz import timezone

from core.StockDbBase import StockDbBase
from core.QueueItem import QueueItem
from db.Finance import Finance_DB

from request.MarketWatchFuturesRequest import MarketWatchFuturesRequest
from request.MarketWatchForexRequest import MarketWatchForexRequest

FUTURES_URL = 'https://www.marketwatch.com/tools/futures'
FUTURES_MARKETS_URL = 'https://www.marketwatch.com/tools/markets/futures'


class MarketWatchFuturesScraper(StockDbBase):

    def __init__(self):
        super(MarketWatchFuturesScraper, self).__init__()
        self.db = Finance_DB
        self.last_fetched_symbols = None
        self.scrape_dict = {}

    def get_next_input(self):
        now = datetime.now(timezone('EST'))

        if self.last_fetched_symbols is None or self.last_fetched_symbols.date() != now.date():
            mwfr = MarketWatchFuturesRequest('symbols')
            self.last_fetched_symbols = now
            return QueueItem.from_request(mwfr, self.process_data, metadata={'symbols': True})

    def process_symbols(self, symbols):
        documents = []
        for symbol in symbols:
            if symbol['symbol']


    def process_data(self, queue_item):
        if queue_item.get_response().status_code != 200:
            return

        metadata = queue_item.get_metadata()
        if metadata['symbols'] is True:
            symbols = MarketWatchFuturesRequest.parse_response(queue_item.get_response().get_data())
            documents = self.process_symbols(symbols)



