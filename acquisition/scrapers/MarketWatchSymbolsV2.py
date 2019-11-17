from datetime import datetime
from pytz import timezone

from core.StockDbBase import StockDbBase
from core.QueueItem import QueueItem
from core.data.SymbolRepository import Symbol_Repository
from request.MarketWatchSymbolsRequestV2 import MarketWatchSymbolsRequestV2
from request.MarketWatchRequestConstants import COUNTRIES, INSTRUMENT_TYPES

#https://quotes.wsj.com/company-list/a-z/W

class MarketWatchSymbolsV2(StockDbBase):

    def __init__(self):
        super(MarketWatchSymbolsV2, self).__init__()
        self.symbol_repository = Symbol_Repository
        self.today = datetime.now(timezone('EST'))
        self.build_scrape_dict()

    def build_scrape_dict(self):
        scrape_dict = {}
        for instrument_type in INSTRUMENT_TYPES:
            scrape_dict[instrument_type] = {country: [0, False] for country in COUNTRIES}
        self.scrape_dict = scrape_dict

    def get_next_input(self):
        now = datetime.now(timezone('EST'))

        if now.date() != self.today:
            self.today = now.date()
            self.build_scrape_dict()

        for instrument_type in INSTRUMENT_TYPES:
            if instrument_type.lower() == 'futures':
                continue
            for country in COUNTRIES:
                if self.scrape_dict[instrument_type][country][0] == 0 or self.scrape_dict[instrument_type][country][1] is True:
                    next_page = self.scrape_dict[instrument_type][country][0] + 1
                    self.scrape_dict[instrument_type][country] = [next_page, False]
                    request = MarketWatchSymbolsRequestV2(country, next_page, instrument_type)
                    return QueueItem(
                        url=request.get_url(),
                        http_method=request.get_http_method(),
                        callback=self.process_data,
                        metadata={'page': next_page, 'country': country, 'instrument_type': instrument_type}
                    )

    def process_data(self, queue_item):
        if not queue_item.get_response().is_successful():
            return

        data = MarketWatchSymbolsRequestV2.parse_response(queue_item.get_response().get_data())
        if data:
            metadata = queue_item.get_metadata()
            country = metadata['country']
            instrument_type = metadata['instrument_type']
            self.scrape_dict[instrument_type][country][1] = True

            documents = []
            for d in data:
                d['country'] = country
                d['instrument_type'] = instrument_type
                cursor = Symbol_Repository.find(symbol=d['symbol'], exchange=d['Exchange'])
                if cursor.count() == 0:
                    documents.append(d)

            if documents:
                Symbol_Repository.insert(documents)

if __name__ == '__main__':
    a = {'test': 'test'}