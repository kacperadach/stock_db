from datetime import datetime

from pytz import timezone

from acquisition.symbol.futures import FUTURES
from core.BaseScraper import BaseScraper
from core.QueueItem import QueueItem
from core.data.QuoteRepository import Quote_Repository
from db.Finance import Finance_DB
from request.MarketWatchRequest import MarketWatchRequest
from request.MarketWatchRequestIndicators import MarketWatchRequestIndicators

# https://www.marketwatch.com/investing/future/{} could  scrape all 1-3 letter + 00



# {"Step":"PT1M","TimeFrame":"D1","EntitlementToken":"cecc4267a0194af89ca343805a3e57af","IncludeMockTick":true,"FilterNullSlots":false,"FilterClosedPoints":true,"IncludeClosedSlots":false,"IncludeOfficialClose":true,"InjectOpen":false,"ShowPreMarket":false,"ShowAfterHours":false,"UseExtendedTimeFrame":false,"WantPriorClose":true,"IncludeCurrentQuotes":false,"ResetTodaysAfterHoursPercentChange":false,"Series":[{"Key":"FUTURE/UK/IFEU/BRN00","Dialect":"Charting","Kind":"Ticker","SeriesId":"s1","DataTypes":["Last"],"Indicators":[{"Parameters":[{"Name":"ShowOpen"},{"Name":"ShowHigh"},{"Name":"ShowLow"},{"Name":"ShowPriorClose","Value":true},{"Name":"Show52WeekHigh"},{"Name":"Show52WeekLow"}],"Kind":"OpenHighLowLines","SeriesId":"i2"}]}]}

class FuturesScraper(BaseScraper):

    COLLECTION_NAME = 'market_watch_futures'

    def __init__(self):
        super(FuturesScraper, self).__init__()
        self.db = Finance_DB
        self.scrape_dict = {}
        self.indicators = MarketWatchRequestIndicators(use_default=True)
        self.today = datetime.now(timezone('EST')).date()
        self.symbols_cursor = self.get_symbols()
        self.quote_repository = Quote_Repository

    def get_symbols(self):
        # FUTURE / US / XNYM / GC00
        return FUTURES

        # return self.db.find(self.MARKET_WATCH_SYMBOL_COLLECTION, {'instrument_type': 'futures'}, {'symbol': 1, 'instrument_type': 1, 'exchange': 1, 'country': 1, 'country_code': 1})

    def get_next_input(self):
        now = datetime.now(timezone('EST'))

        if now.date() != self.today:
            self.today = now.date()
            self.scrape_dict = {}
            self.symbols_cursor = self.get_symbols()

        for symbol in self.symbols_cursor:
            unique_id = self.get_unique_id(symbol['symbol'], symbol['instrument_type'], symbol['exchange'])
            if unique_id not in self.scrape_dict.keys():
                self.scrape_dict[unique_id] = True
                mwr = MarketWatchRequest(symbol=symbol, step_interval='1d', instrument_type=symbol['instrument_type'], indicators=self.indicators)
                return QueueItem(url=mwr.get_url(), http_method=mwr.get_http_method(), headers=mwr.get_headers(), callback=self.process_data,  metadata={'symbol': symbol, 'type': 'historical', 'indicators': self.indicators})

    def process_data(self, queue_item):
        super(FuturesScraper, self).process_data(queue_item)

        data = MarketWatchRequest.parse_response(queue_item.get_response().get_data())
        if not data:
            self.log('Future not found: {}'.format(queue_item.get_metadata()))

        if data:
            self.quote_repository.insert(data, queue_item.get_metadata())

    # for the purpose of storing in scrape_dict
    def get_unique_id(self, symbol, instrument_type, exchange):
        return str(symbol) + str(instrument_type) + str(exchange)


