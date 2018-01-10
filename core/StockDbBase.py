import traceback

from logger import Logger

class StockDbBase(object):
    def __init__(self):
        pass

    def log(self, msg, level='info'):
        Logger.log(msg, level=level, threadname=self.__class__.__name__)

    def log_exception(self, exception):
        Logger.log("Unexpected error occurred", level='error', threadname=self.__class__.__name__)
        Logger.log(traceback.format_exc(exception))
