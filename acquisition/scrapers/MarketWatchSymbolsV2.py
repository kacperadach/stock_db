from datetime import datetime, timedelta
from pytz import timezone

from core.BaseScraper import BaseScraper
from core.QueueItem import QueueItem
from core.data.SymbolRepository import SymbolRepository
from request.MarketWatchSymbolsRequestV2 import MarketWatchSymbolsRequestV2
from request.MarketWatchRequestConstants import COUNTRIES, INSTRUMENT_TYPES

#https://quotes.wsj.com/company-list/a-z/W


MAX_PAGES = 250

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
                for i in range(1, MAX_PAGES + 1):
                    yield {
                        'instrument_type': instrument_type,
                        'country': country,
                        'page': i
                    }

    def get_queue_item(self, symbol):
        page = symbol['page']
        country = symbol['country']
        instrument_type = symbol['instrument_type']

        request = MarketWatchSymbolsRequestV2(country, page, instrument_type)
        return QueueItem(
            url=request.get_url(),
            http_method=request.get_http_method(),
            callback=__name__,
            metadata={'page': page, 'country': country, 'instrument_type': instrument_type}
        )

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
                documents.append(d)

                # cursor = Symbol_Repository.find(symbol=d['symbol'], exchange=d['Exchange'])
                # if cursor.count() == 0:
                #     documents.append(d)

            if documents:
                self.symbol_repository.insert(documents)

if __name__ == '__main__':
    a = {'test': 'test'}