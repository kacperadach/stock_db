import sys
from abc import abstractmethod
import traceback

from datetime import datetime
from pytz import timezone

from core.RateLimiter import RateLimiter
from core.StockDbBase import StockDbBase

class BaseScraper(StockDbBase):
    MARKET_WATCH_SYMBOL_COLLECTION = 'market_watch_symbols'

    def __init__(self):
        super(BaseScraper, self).__init__()
        self.rate_limiter = RateLimiter(self.requests_per_second())
        self.last_scrape = timezone('EST').localize(datetime.min)
        self.symbols_cursor = None
        self.additional_symbols = []

    @abstractmethod
    def get_symbols(self):
        raise NotImplementedError('get_symbols')

    @abstractmethod
    def get_queue_item(self, symbol):
        raise NotImplementedError('get_queue_item')

    @abstractmethod
    def get_time_delta(self):
        raise NotImplementedError('get_time_delta')

    @abstractmethod
    def process_data(self, queue_item):
        raise NotImplementedError('process_data')

    @abstractmethod
    def should_scrape(self):
        return True

    @abstractmethod
    def requests_per_second(self):
        return sys.maxsize

    @abstractmethod
    def request_callback(self, queue_item):
        pass

    # dont override
    @abstractmethod
    def callback(self, queue_item, log_queue):
        try:
            self.request_callback(queue_item)
        except Exception:
            log_queue.put('Error occurred in callback:\n' + traceback.format_exc())

    # Scraper Core Logic
    @abstractmethod
    def get_next_input(self):
        now = datetime.now(timezone('EST'))

        if self.symbols_cursor is None and self.last_scrape + self.get_time_delta() < now:
            self.symbols_cursor = iter(list(self.get_symbols()))

        if self.additional_symbols and not self.rate_limiter.is_rate_limited():
            return self.get_queue_item(self.additional_symbols.pop(0))

        if self.symbols_cursor is not None and self.should_scrape() and not self.rate_limiter.is_rate_limited():
            try:
                return self.get_queue_item(next(self.symbols_cursor))
            except StopIteration:
                self.last_scrape = now
                self.symbols_cursor = None
