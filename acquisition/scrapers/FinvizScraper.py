from datetime import timedelta

from core.BaseScraper import BaseScraper
from core.QueueItem import QueueItem
from core.data.FinvizRepository import FinvizRepository
from db.Finance import FinanceDB
from request.FinvizRequest import FinvizRequest

SYMBOL_COLLECTION = 'market_watch_symbols'

class FinvizScraper(BaseScraper):

    def __init__(self):
        super(FinvizScraper, self).__init__()
        self.db = FinanceDB()
        self.finviz_repository = FinvizRepository()

    def get_symbols(self):
        return iter(list(self.db.find(SYMBOL_COLLECTION, {'country_code': 'US', 'instrument_type': {'$in': ['stocks', 'exchange-traded-funds', 'exchange-traded-notes']}}, {'symbol': 1, 'instrument_type': 1, 'exchange': 1, 'country': 1, 'country_code': 1})))

    def get_queue_item(self, symbol):
        finviz = FinvizRequest(symbol['symbol'])
        return QueueItem(url=finviz.get_url(), http_method=finviz.get_http_method(), metadata=symbol, callback=__name__)

    def get_time_delta(self):
        return timedelta(hours=6)

    def process_data(self, queue_item):
        if not queue_item.get_response().is_successful():
            return

        data = FinvizRequest.parse_response(queue_item.get_response().get_data())
        if any(data.values()):
            self.finviz_repository.insert(data, queue_item.get_metadata(), queue_item.get_utc_time())
