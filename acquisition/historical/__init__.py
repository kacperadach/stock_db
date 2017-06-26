import threading
from datetime import datetime, timedelta
import traceback
import time

from logger import Logger
from acquisition.historical.stocks import HistoricalStockAcquisition

class HistoricalAcquisition(threading.Thread):

    def __init__(self, AcquirerThread):
        super(HistoricalAcquisition, self).__init__()
        self.thread_name = 'HistoricalAcquisition'
        self.AcquirerThread = AcquirerThread
        self.HistoricalStockAcquisition = HistoricalStockAcquisition()
        self.date = datetime.now().date()

    def _log(self, msg, level='info'):
        Logger.log(msg, level=level, threadname=self.thread_name)

    def run(self):
        try:
            while 1:
                self.AcquirerThread.event.wait()
                self._log('Beginning Acquisition')
                self.acquire()
                self._log('Waiting for Acquirer to finish execution')
        except Exception as e:
            self._log('unexpected error occured: {}'.format(e))
            Logger.log(traceback.format_exc())

    def acquire(self):
        while self.AcquirerThread.event.is_set():
            try:
                self.HistoricalStockAcquisition.next()
            except StopIteration:
                today = datetime.now().date()
                if self.date == today:
                    tomorrow = today + timedelta(days=1)
                    tomorrow = datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day)
                    sleep_time = (tomorrow - datetime.now()).total_seconds()
                    self._log('Sleeping for ' + str(timedelta(seconds=sleep_time)))
                    time.sleep(sleep_time)
                else:
                    self.date = datetime.now().date()
