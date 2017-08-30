import sys
import threading
from datetime import datetime, timedelta
import traceback
import time


from logger import Logger
from acquisition.historical.stocks import HistoricalStockAcquisition
from acquisition.historical.commodities import HistoricalCommoditiesAcquisition
from acquisition.historical.currencies import HistoricalCurrenciesAcquisition
from acquisition.historical.financials.financials import Financials
from discord.webhook import DiscordWebhook

class HistoricalAcquisition(threading.Thread):

    def __init__(self, AcquirerThread=None):
        super(HistoricalAcquisition, self).__init__()
        self.thread_name = 'HistoricalAcquisition'
        self.AcquirerThread = AcquirerThread
        self.date = datetime.now().date()
        self.today = datetime.now().date()
        self.tasks = (HistoricalStockAcquisition(), HistoricalCommoditiesAcquisition(), HistoricalCurrenciesAcquisition())
        self.task_counter = 0
        self.finished = False

    def _log(self, msg, level='info'):
        Logger.log(msg, level=level, threadname=self.thread_name)

    def run(self):
        try:
            while 1:
                if self.AcquirerThread is None:
                    self._log('Beginning Acquisition')
                    self.acquire()
                    self._sleep()
                else:
                    self._log('Waiting for Acquirer to finish execution')
                    self.AcquirerThread.event.wait()
                    self._log('Beginning Acquisition')
                    while self.AcquirerThread.event.is_set():
                        self.acquire()
                        self._sleep()
        except Exception as e:
            self._log('unexpected error occured: {}'.format(e))
            self._log(traceback.format_exc(), level='error')
            if Logger.env == 'prod':
                DiscordWebhook().alert_error(self.thread_name, traceback.format_exc())

    def _sleep(self):
        if self.finished:
            tomorrow = self.today + timedelta(days=1)
            tomorrow = datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day)
            self.sleep_time = (tomorrow - datetime.now()).total_seconds()
            self._log('Sleeping for ' + str(timedelta(seconds=self.sleep_time)))
            time.sleep(self.sleep_time)
        else:
            self._log("Not sleeping")

    def _call_next(self):
        current_task = self.tasks[self.task_counter]
        try:
            current_task.next()
        except StopIteration:
            self._log('{} task completed'.format(current_task.task_name))
            if self.task_counter == len(self.tasks) - 1:
                return True
            else:
                self.task_counter += 1
                return False
        return False

    def acquire(self):
        self.finished = False
        while 1 if not self.AcquirerThread else self.AcquirerThread.event.is_set():
            finished = self._call_next()
            if finished:
                self.task_counter = 0
                self.today = datetime.now().date()
                if self.date == self.today:
                    self.finished = True
                    break
                else:
                    self.date = datetime.now().date()
                    break

if __name__ == "__main__":
    from acquisition.Acquirer import Acquirer
    # a = Acquirer()
    # a.start()
    HistoricalAcquisition().start()