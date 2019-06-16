from abc import abstractmethod

from core.QueueItem import QueueItem
from core.StockDbBase import StockDbBase

class BaseScraper(StockDbBase):

    @abstractmethod
    def get_next_input(self):
        pass

    @abstractmethod
    def process_data(self, queue_item):
        if not isinstance(queue_item, QueueItem):
            raise AssertionError('need queue_item in process_data')
