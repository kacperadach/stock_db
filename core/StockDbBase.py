import traceback

from logger import AppLogger

class StockDbBase(object):

    def __init__(self):
        self.logger = AppLogger()

    def log(self, msg, level='info'):
        self.logger.log(msg, level=level, threadname=self.__class__.__name__)

    def log_exception(self, exception):
        self.logger.log("Unexpected error occurred", level='error', threadname=self.__class__.__name__)
        self.logger.log(traceback.format_exc(exception))

    def log_queue_item(self, queue_item):
        self.logger.log('Queue Item causing exception: {}'.format(str(queue_item)))
