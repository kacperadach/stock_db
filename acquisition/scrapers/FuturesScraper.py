from datetime import timedelta

from core.BaseScraper import BaseScraper
from core.QueueItem import QueueItem
from core.data.FuturesRepository import FuturesRepository
from core.data.QuoteRepository import Quote_Repository
from request.MarketWatchRequest import MarketWatchRequest
from request.MarketWatchRequestIndicators import MarketWatchRequestIndicators

# https://www.marketwatch.com/investing/future/{} could  scrape all 1-3 letter + 00
# {"Step":"PT1M","TimeFrame":"D1","EntitlementToken":"cecc4267a0194af89ca343805a3e57af","IncludeMockTick":true,"FilterNullSlots":false,"FilterClosedPoints":true,"IncludeClosedSlots":false,"IncludeOfficialClose":true,"InjectOpen":false,"ShowPreMarket":false,"ShowAfterHours":false,"UseExtendedTimeFrame":false,"WantPriorClose":true,"IncludeCurrentQuotes":false,"ResetTodaysAfterHoursPercentChange":false,"Series":[{"Key":"FUTURE/UK/IFEU/BRN00","Dialect":"Charting","Kind":"Ticker","SeriesId":"s1","DataTypes":["Last"],"Indicators":[{"Parameters":[{"Name":"ShowOpen"},{"Name":"ShowHigh"},{"Name":"ShowLow"},{"Name":"ShowPriorClose","Value":true},{"Name":"Show52WeekHigh"},{"Name":"Show52WeekLow"}],"Kind":"OpenHighLowLines","SeriesId":"i2"}]}]}

class FuturesScraper(BaseScraper):

    COLLECTION_NAME = 'market_watch_futures'

    def __init__(self):
        super(FuturesScraper, self).__init__()
        self.indicators = MarketWatchRequestIndicators(use_default=True)
        self.quote_repository = Quote_Repository

    def get_symbols(self):
        return iter(FuturesRepository.get_all_futures())

    def get_queue_item(self, symbol):
        mwr = MarketWatchRequest(symbol=symbol, step_interval='1d', instrument_type=symbol['instrument_type'], indicators=self.indicators)
        return QueueItem(url=mwr.get_url(), http_method=mwr.get_http_method(), headers=mwr.get_headers(), callback=__name__, metadata={'symbol': symbol, 'type': 'historical', 'indicators': self.indicators})

    def get_time_delta(self):
        return timedelta(minutes=5)

    def process_data(self, queue_item):
        data = MarketWatchRequest.parse_response(queue_item.get_response().get_data())

        if data:
            self.quote_repository.insert(data, queue_item.get_metadata())


class Futures1mScraper(FuturesScraper):

    def __init__(self):
        super(Futures1mScraper, self).__init__()

    def get_queue_item(self, symbol):
        mwr = MarketWatchRequest(symbol=symbol, step_interval='1m', instrument_type=symbol['instrument_type'], indicators=self.indicators)
        return QueueItem(url=mwr.get_url(), http_method=mwr.get_http_method(), headers=mwr.get_headers(), callback=self.process_data, metadata={'symbol': symbol, 'indicators': self.indicators})

