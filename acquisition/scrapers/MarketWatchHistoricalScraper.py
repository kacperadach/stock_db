from datetime import timedelta

from core.BaseScraper import BaseScraper
from core.QueueItem import QueueItem
from core.data.QuoteRepository import QuoteRepository
from db.Finance import FinanceDB
from request.MarketWatchRequest import MarketWatchRequest
from request.MarketWatchRequestIndicators import MarketWatchRequestIndicators

SYMBOL_COLLECTION = 'market_watch_symbols'

class MarketWatchHistoricalScraper(BaseScraper):

    def __init__(self, indicators=None):
        super(MarketWatchHistoricalScraper, self).__init__()
        self.db = FinanceDB()
        self.scrape_dict = {}
        self.quote_repository = QuoteRepository()
        if indicators is None:
            indicators = MarketWatchRequestIndicators(use_default=True)
        self.indicators = indicators

    def get_symbols(self):
        return iter(list(self.db.find(SYMBOL_COLLECTION, {}, {'symbol': 1, 'instrument_type': 1, 'exchange': 1, 'country': 1, 'country_code': 1, 'charting_symbol': 1})))

    def get_time_delta(self):
        return timedelta(hours=12)

    def get_queue_item(self, symbol):
        if 'charting_symbol' not in symbol.keys() and symbol['instrument_type'] == 'unknown':
            return None

        mwr = MarketWatchRequest(symbol=symbol, step_interval='1d', instrument_type=symbol['instrument_type'], indicators=self.indicators)
        return QueueItem(url=mwr.get_url(), http_method=mwr.get_http_method(), headers=mwr.get_headers(), callback=__name__, metadata={'symbol': symbol, 'type': 'historical', 'indicators': self.indicators})

    def process_data(self, queue_item):
        if not queue_item.get_response().is_successful():
            return

        data = MarketWatchRequest.parse_response(queue_item.get_response().get_data())

        if not data:
            return

        metadata = queue_item.get_metadata()
        # if metadata['symbol']['symbol'] != data['symbol']:
        #     return

        self.quote_repository.insert(data, metadata)
