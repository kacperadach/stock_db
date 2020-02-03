from copy import deepcopy
from datetime import datetime, timedelta
from pytz import timezone

from core.BaseScraper import BaseScraper
from core.QueueItem import QueueItem
from core.data.SymbolRepository import SymbolRepository
from request.MarketWatchSymbolsRequestV2 import MarketWatchSymbolsRequestV2
from request.MarketWatchRequestConstants import COUNTRIES, INSTRUMENT_TYPES

#https://quotes.wsj.com/company-list/a-z/W


class MarketWatchSymbolsV2(BaseScraper):

    def __init__(self):
        super(MarketWatchSymbolsV2, self).__init__()
        self.symbol_repository = SymbolRepository()
        self.today = datetime.now(timezone('EST'))

    def get_time_delta(self):
        return timedelta(days=3)

    def get_symbols(self):
        for instrument_type in INSTRUMENT_TYPES:
            if instrument_type.lower() == 'futures':
                continue
            for country in COUNTRIES:
                yield {
                    'instrument_type': instrument_type,
                    'country': country,
                    'page': 1
                }

    def get_queue_item(self, symbol):
        page = symbol['page']
        country = symbol['country']
        instrument_type = symbol['instrument_type']

        request = MarketWatchSymbolsRequestV2(country, page, instrument_type)
        return QueueItem(
            url=request.get_url(),
            http_method=request.get_http_method(),
            metadata={'page': page, 'country': country, 'instrument_type': instrument_type},
            callback=__name__
        )

    def request_callback(self, queue_item):
        if MarketWatchSymbolsRequestV2.parse_response(queue_item.get_response().get_data()):
            meta = deepcopy(queue_item.get_metadata())
            meta['page'] += 1
            self.additional_symbols.append(meta)

    def process_data(self, queue_item):
        if not queue_item.get_response().is_successful():
            return

        data = MarketWatchSymbolsRequestV2.parse_response(queue_item.get_response().get_data())
        if data:
            metadata = queue_item.get_metadata()
            country = metadata['country']
            instrument_type = metadata['instrument_type']

            documents = []
            for d in data:
                d['country'] = country
                d['instrument_type'] = instrument_type
                if 'Sector' in d.keys():
                    if d['Sector'] == 'Exchange-Traded Funds':
                        d['instrument_type'] = 'exchange-traded-funds'
                existing = list(self.symbol_repository.find(symbol=d['symbol'], exchange=d['Exchange'], instrument_type=d['instrument_type']))
                if len(existing) == 0:
                    documents.append(d)

            if documents:
                self.symbol_repository.insert(documents)

if __name__ == '__main__':
    a = {'test': 'test'}

    # ScraperQueueManager: Error
    # occurred in callback
    # for metadata {'page': 1, 'country': 'japan', 'instrument_type': 'exchange-traded-notes'}:
    #     Traceback(most
    #     recent
    #     call
    #     last):
    #     File
    #     "/root/stock_db/core/BaseScraper.py", line
    #     53, in callback
    #     self.request_callback(queue_item)
    # File
    # "/root/stock_db/acquisition/scrapers/MarketWatchSymbolsV2.py", line
    # 49, in request_callback
    # if MarketWatchSymbolsRequestV2.parse_response(queue_item.get_response().get_data()):
    #     File
    # "/root/stock_db/request/MarketWatchSymbolsRequestV2.py", line
    # 34, in parse_response
    # bs = BeautifulSoup(response, 'html.parser', parse_only=SoupStrainer('table'))
    # File
    # "/usr/local/lib/python3.6/dist-packages/bs4/__init__.py", line
    # 310, in __init__
    # markup, from_encoding, exclude_encodings = exclude_encodings)):
    # File
    # "/usr/local/lib/python3.6/dist-packages/bs4/builder/_htmlparser.py", line
    # 248, in prepare_markup
    # exclude_encodings = exclude_encodings)
    # File
    # "/usr/local/lib/python3.6/dist-packages/bs4/dammit.py", line
    # 381, in __init__
    # markup, override_encodings, is_html, exclude_encodings)
    # File
    # "/usr/local/lib/python3.6/dist-packages/bs4/dammit.py", line
    # 249, in __init__
    # self.markup, self.sniffed_encoding = self.strip_byte_order_mark(markup)
    # File
    # "/usr/local/lib/python3.6/dist-packages/bs4/dammit.py", line
    # 309, in strip_byte_order_mark
    # elif data[:3] == b'\xef\xbb\xbf':
    # TypeError: unhashable
    # type: 'slice'