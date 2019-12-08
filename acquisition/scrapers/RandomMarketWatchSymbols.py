import string
from datetime import timedelta

from core.BaseScraper import BaseScraper
from core.QueueItem import QueueItem
from core.data.SymbolRepository import SymbolRepository

from request.MWSearchRequest import MWSearchRequest


alphabet = string.ascii_lowercase

class RandomMarketWatchSymbols(BaseScraper):

    def __init__(self):
        super(RandomMarketWatchSymbols, self).__init__()
        self.symbol_repository = SymbolRepository()

    def get_symbols(self):
        for i1 in alphabet:
            yield i1

        for i1 in alphabet:
            for i2 in alphabet:
                yield i1 + i2

        for i1 in alphabet:
            for i2 in alphabet:
                for i3 in alphabet:
                    yield i1 + i2 + i3

        for i1 in alphabet:
            for i2 in alphabet:
                for i3 in alphabet:
                    for i4 in alphabet:
                        yield i1 + i2 + i3 + i4

    def get_queue_item(self, symbol):
        request = MWSearchRequest(symbol)
        return QueueItem(url=request.get_url(), http_method=request.get_http_method(), callback=self.process_data, metadata={'symbol': symbol})

    def get_time_delta(self):
        return timedelta(days=3)

    def process_data(self, queue_item):
        data = MWSearchRequest.parse_response(queue_item.get_response().get_data())

        if data:
            new_symbols = []
            for d in data:
                existing = list(self.symbol_repository.find(symbol=d['symbol'], exchange=d['exchange'], instrument_type=d['instrument_type']))
                if len(existing) == 0:
                    new_symbols.append(d)
                else:
                    e = existing[0]
                    if 'charting_symbol' not in e.keys() and 'charting_symbol' in d.keys():
                        e['charting_symbol'] = d['charting_symbol']
                        new_symbols.append(e)

            if new_symbols:
                self.symbol_repository.insert(new_symbols)
