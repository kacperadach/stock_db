import threading
import time
import datetime
import traceback

from logger import Logger
from acquisition.historical.stocks import HistoricalStockAcquisition

class HistoricalAcquisition(threading.Thread):

    def __init__(self, AcquirerThread):
        super(HistoricalAcquisition, self).__init__()
        self.thread_name = 'HistoricalAcquisition'
        self.AcquirerThread = AcquirerThread
        self.HistoricalStockAcquisition = HistoricalStockAcquisition()

    def _log(self, msg, level='info'):
        Logger.log(msg, level=level, threadname=self.thread_name)

    def run(self):
        try:
            while 1:
                #while not self.AcquirerThread.event.is_set():
                self.AcquirerThread.event.wait()
                self.acquire()
        except Exception as e:
            self._log('unexpected error occured: {}'.format(e))
            Logger.log(traceback.format_exc())

    def acquire(self):
        while self.AcquirerThread.event.is_set():
            self.HistoricalStockAcquisition.next()


