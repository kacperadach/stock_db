from datetime import datetime, timedelta

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
        self.symbols_cursor = None
        self.quote_repository = Quote_Repository
        self.last_scrape = timezone('EST').localize(datetime.min)

    def get_symbols(self):
        return iter([symbol for _, symbols in FUTURES.iteritems() for symbol in symbols])

    def get_queue_item(self, symbol):
        mwr = MarketWatchRequest(symbol=symbol, step_interval='1d', instrument_type=symbol['instrument_type'], indicators=self.indicators)
        return QueueItem(url=mwr.get_url(), http_method=mwr.get_http_method(), headers=mwr.get_headers(), callback=self.process_data, metadata={'symbol': symbol, 'type': 'historical', 'indicators': self.indicators})

    def get_time_delta(self):
        return timedelta(minutes=4)

    # Scraper Core Logic
    def get_next_input(self):
        now = datetime.now(timezone('EST'))

        if self.symbols_cursor is None and self.last_scrape + self.get_time_delta() < now:
            self.symbols_cursor = self.get_symbols()

        if self.symbols_cursor is not None:
            for symbol in self.symbols_cursor:
                self.log('PAULY D')
                return self.get_queue_item(symbol)

            self.last_scrape = now
            self.symbols_cursor = None


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


