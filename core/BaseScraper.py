from abc import abstractmethod

from core.QueueItem import QueueItem
from core.StockDbBase import StockDbBase

class BaseScraper(StockDbBase):
    MARKET_WATCH_SYMBOL_COLLECTION = 'market_watch_symbols'

    @abstractmethod
    def get_symbols(self):
        raise NotImplementedError('get_symbols')

    @abstractmethod
    def get_next_input(self):
        raise NotImplementedError('get_next_input')

    @abstractmethod
    def process_data(self, queue_item):
        if not isinstance(queue_item, QueueItem):
            raise AssertionError('need queue_item in process_data')
