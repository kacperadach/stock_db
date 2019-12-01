from abc import abstractmethod

from datetime import datetime
from pytz import timezone

from core.StockDbBase import StockDbBase

class BaseScraper(StockDbBase):
    MARKET_WATCH_SYMBOL_COLLECTION = 'market_watch_symbols'

    last_scrape = timezone('EST').localize(datetime.min)
    symbols_cursor = None

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
    def should_scrape(self, now):
        return True

    # Scraper Core Logic
    @abstractmethod
    def get_next_input(self):
        now = datetime.now(timezone('EST'))

        if self.symbols_cursor is None and self.last_scrape + self.get_time_delta() < now:
            self.symbols_cursor = self.get_symbols()
            # check if get_symbols returned iter

        if self.symbols_cursor is not None and self.should_scrape(now):
            try:
                return self.get_queue_item(next(self.symbols_cursor))
            except StopIteration:
                self.last_scrape = now
                self.symbols_cursor = None
