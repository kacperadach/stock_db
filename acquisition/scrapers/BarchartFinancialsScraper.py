from datetime import timedelta
from copy import deepcopy

from core.BaseScraper import BaseScraper
from core.QueueItem import QueueItem
from core.data.BarchartFinancialsRepository import BarchartFinancialsRepository
from request.BarchartFinancialsRequest import BarchartFinancialsRequest, PERIODS, DOCUMENT_TYPES


class BarchartFinancialsScraper(BaseScraper):

    def __init__(self):
        super(BarchartFinancialsScraper, self).__init__()
        self.repository = BarchartFinancialsRepository()

    def get_symbols(self):
        symbols = list(self.repository.get_symbols())
        for period in PERIODS:
            for document_type in DOCUMENT_TYPES:
                for symbol in symbols:
                    yield {
                        'period': period,
                        'document_type': document_type,
                        'symbol': symbol['symbol'],
                        'exchange': symbol['exchange'],
                        'page': 1
                    }

    def get_queue_item(self, symbol):
        request = BarchartFinancialsRequest(symbol['symbol'], symbol['document_type'], symbol['period'])
        return QueueItem(url=request.get_url(), headers=request.get_headers(), http_method=request.get_http_method(), callback=__name__, metadata=symbol)

    def get_time_delta(self):
        return timedelta(hours=20)

    def requests_per_second(self):
        return 2

    def request_callback(self, queue_item):
        if not queue_item.get_response().is_successful():
            return

        next_page = BarchartFinancialsRequest.get_next_page(queue_item.get_response().get_data())
        if next_page is not None:
            meta = deepcopy(queue_item.get_metadata())
            meta['page'] = next_page
            self.additional_symbols.append(meta)

    def process_data(self, queue_item):
        if not queue_item.get_response().is_successful():
            return

        data = BarchartFinancialsRequest.parse_response(queue_item.get_response().get_data())
        if data:
            self.repository.insert(data, queue_item.get_metadata())
