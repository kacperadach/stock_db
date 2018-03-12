from datetime import datetime
from pytz import timezone

from core.StockDbBase import StockDbBase
from core.QueueItem import QueueItem
from db.Finance import Finance_DB
from request.MarketWatchSymbolsRequest import MarketWatchSymbolsRequest, TYPE_OPTIONS

COLLECTION_NAME = 'market_watch_symbols'

class MarketWatchSymbols(StockDbBase):

    def __init__(self):
        super(MarketWatchSymbols, self).__init__()
        self.db = Finance_DB
        self.today = datetime.now(timezone('EST'))
        self.scrape_dict = {}
        self.letter_options = MarketWatchSymbolsRequest.get_letter_options()
        self.instrument_types = list(TYPE_OPTIONS)
        # futures contracts have wrong symbols on MarketWatch
        self.instrument_types.remove('futures')

    def get_next_input(self):
        now = datetime.now(timezone('EST'))

        if now.date() != self.today:
            self.today = now.date()
            self.scrape_dict = {}

        for instrument_type in self.instrument_types:
            for letter in self.letter_options:
                if instrument_type not in self.scrape_dict.keys():
                    self.scrape_dict[instrument_type] = {letter: {'attempted': 1, 'successful': 0}}
                    mwsr = MarketWatchSymbolsRequest(instrument_type, letter, 1)
                    return QueueItem(
                        url=mwsr.get_url(),
                        http_method=mwsr.get_http_method(),
                        callback=self.process_data,
                        metadata={'instrument_type': instrument_type, 'letter': letter, 'page': 1}
                    )
                elif letter not in self.scrape_dict[instrument_type].keys():
                    self.scrape_dict[instrument_type][letter] = {'attempted': 1, 'successful': 0}
                    mwsr = MarketWatchSymbolsRequest(instrument_type, letter, 1)
                    return QueueItem(
                        url=mwsr.get_url(),
                        http_method=mwsr.get_http_method(),
                        callback=self.process_data,
                        metadata={'instrument_type': instrument_type, 'letter': letter, 'page': 1}
                    )
                elif self.scrape_dict[instrument_type][letter]['attempted'] == self.scrape_dict[instrument_type][letter]['successful']:
                    self.scrape_dict[instrument_type][letter]['attempted'] += 1
                    page = self.scrape_dict[instrument_type][letter]['attempted']
                    mwsr = MarketWatchSymbolsRequest(instrument_type, letter, page)
                    return QueueItem(
                        url=mwsr.get_url(),
                        http_method=mwsr.get_http_method(),
                        callback=self.process_data,
                        metadata={'instrument_type': instrument_type, 'letter': letter, 'page': page}
                    )

    def process_data(self, queue_item):
        if not queue_item.get_response().is_successful():
            return

        data = MarketWatchSymbolsRequest.parse_response(queue_item.get_response().get_data())
        metadata = queue_item.get_metadata()
        instrument_type = metadata['instrument_type']
        letter = metadata['letter']
        page = metadata['page']
        if data and self.scrape_dict[instrument_type][letter]['successful'] < page:
            self.scrape_dict[instrument_type][letter]['successful'] = page

        documents = []
        symbol_and_instrument_type = []
        for d in data:
            d['instrument_type'] = instrument_type
            cursor = self.db.find(COLLECTION_NAME, {'instrument_type': instrument_type, 'symbol': d['symbol'], 'Exchange': d['Exchange']}, {})
            if cursor.count() == 0 and d['symbol'] + d['instrument_type'] not in symbol_and_instrument_type:
                documents.append(d)
                symbol_and_instrument_type.append(d['symbol'] + d['instrument_type'])

        if documents:
            self.db.insert(COLLECTION_NAME, documents)
