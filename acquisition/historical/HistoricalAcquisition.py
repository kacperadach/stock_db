import threading
from datetime import datetime, timedelta
import traceback
import time

from logger import Logger
from acquisition.historical.stocks import HistoricalStockAcquisition
from acquisition.historical.commodities import HistoricalCommoditiesAcquisition
from acquisition.historical.currencies import HistoricalCurrenciesAcquisition
from discord.webhook import DiscordWebhook

class HistoricalAcquisition(threading.Thread):

    def __init__(self, AcquirerThread):
        super(HistoricalAcquisition, self).__init__()
        self.thread_name = 'HistoricalAcquisition'
        self.AcquirerThread = AcquirerThread
        self.date = datetime.now().date()
        self.today = datetime.now().date()
        self.tasks = (HistoricalStockAcquisition(), HistoricalCommoditiesAcquisition(), HistoricalCurrenciesAcquisition())
        self.task_counter = 0

    def _log(self, msg, level='info'):
        Logger.log(msg, level=level, threadname=self.thread_name)

    def run(self):
        try:
            while 1:
                self.AcquirerThread.event.wait()
                self._log('Beginning Acquisition')
                while self.AcquirerThread.event.is_set():
                    self.acquire()
                    self._sleep()
                self._log('Waiting for Acquirer to finish execution')
        except Exception as e:
            self._log('unexpected error occured: {}'.format(e))
            Logger.log(traceback.format_exc())
            if Logger.env == 'prod':
                DiscordWebhook().alert_error(self.thread_name, traceback.format_exc())

    def _sleep(self):
        if self.finished:
            tomorrow = self.today + timedelta(days=1)
            tomorrow = datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day)
            self.sleep_time = (tomorrow - datetime.now()).total_seconds()
            self._log('Sleeping for ' + str(timedelta(seconds=self.sleep_time)))
            time.sleep(self.sleep_time)

    def _call_next(self):
        current_task = self.tasks[self.task_counter]
        try:
            current_task.next()
        except StopIteration:
            if self.task_counter == len(self.tasks) - 1:
                return True
            else:
                self.task_counter += 1
                return False
        return False

    def acquire(self):
        self.finished = False
        while self.AcquirerThread.event.is_set():
            finished = self._call_next()
            if finished:
                self.today = datetime.now().date()
                if self.date == self.today:
                    self.finished = True
                    break
                else:
                    self.date = datetime.now().date()
                    break
