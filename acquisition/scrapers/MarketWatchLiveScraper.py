from copy import deepcopy
from datetime import datetime
from pytz import timezone
import time

from core.BaseScraper import BaseScraper
from core.QueueItem import QueueItem
from core.market.Market import is_market_open
from db.Finance import FinanceDB
from core.data.QuoteRepository import QuoteRepository
from request.MarketWatchRequest import MarketWatchRequest
from request.MarketWatchRequestIndicators import MarketWatchRequestIndicators

SYMBOLS_COLLECTION = 'symbols'
LIVE_SCRAPE_PERIOD_SEC = 900

class MarketWatchLiveScraper(BaseScraper):

    def __init__(self, indicators=None):
        super(MarketWatchLiveScraper, self).__init__()
        self.db = FinanceDB()
        self.quote_repository = QuoteRepository()
        if indicators is None:
            indicators = MarketWatchRequestIndicators(use_default=True)
        self.indicators = indicators

    def get_queue_item(self, symbol):
        mwr = MarketWatchRequest(symbol=symbol, step_interval='1m', instrument_type=symbol['instrument_type'], indicators=self.indicators)
        return QueueItem(url=mwr.get_url(), http_method=mwr.get_http_method(), headers=mwr.get_headers(), metadata={'symbol': symbol, 'time_interval': '1m', 'indicators': self.indicators})

    def process_data(self, queue_item):
        if not queue_item.get_response().is_successful():
            return

        data = MarketWatchRequest.parse_response(queue_item.get_response().get_data())

        if not data:
            return

        metadata = queue_item.get_metadata()
        # if metadata['symbol']['symbol'] != data['symbol']:
        #     return

        if 'time_interval' not in metadata.keys():
            raise RuntimeError('need time_interval in meta')

        self.quote_repository.insert(data, metadata)
