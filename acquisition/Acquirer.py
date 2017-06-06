from datetime import date, datetime, timedelta

from logger import Logger
from app.thread import FThread
from acquisition.daily.options import get_all_options_data
from acquisition.daily.insider import get_all_insider_data
from acquisition.daily.commodities import get_all_commodities_data

class Acquirer(FThread):

    def __init__(self):
        super(Acquirer, self).__init__()
        self.thread_name = 'Acquirer'
        self.trading_day = None
        self.history = {}

    def _run(self):
        self.history.clear()
        self.trading_date = datetime.now()
        if self.trading_date.weekday() > 4:
            self._log('Not running Acquirer on weekend: {}'.format(self.trading_date))
        else:
            if self.trading_date.hour < 16:
                self._log('Trading day has not finished yet, {}'.format(self.trading_date.time()))
            else:
                self.trading_date = self.trading_date.date()
                self._log('using {} as trading day'.format(self.trading_date))

                found, not_found = self.complete_task(get_all_options_data)
                self.assert_complete(get_all_options_data, found, not_found)

                found, not_found = self.complete_task(get_all_insider_data)
                self.assert_complete(get_all_insider_data, found, not_found)

        found, not_found = self.complete_task(get_all_commodities_data)
        self.assert_complete(get_all_commodities_data, found, not_found)

    def complete_task(self, task_fn):
        self._log('beginning {} task'.format(task_fn.func_name))
        self.history[task_fn.func_name] = False
        start = datetime.now()
        found, not_found = task_fn(self.trading_date)
        end = datetime.now()
        self._log('{} task took {}'.format(task_fn.func_name, end - start))
        self._log('acquired data for {} securities.'.format(len(found)))
        return found, not_found

    def assert_complete(self, task_fn, found, not_found):
        if len(found) + len(not_found) == 0:
            self._log('no securities scheduled while completing task {}'.format(task_fn.func_name), level='warning')
        elif len(not_found) > 50:
            self._log('could not find data for {} ( {}% ) securities while completing task {}'.format(len(not_found), round((float(len(not_found))/(len(found)+len(not_found)))*100, 2), task_fn.func_name), level='warning')
        else:
            self.history[task_fn.func_name] = True

    def _sleep(self):
        now = datetime.now()
        if all(self.history.values()):
            if now.hour < 16:
                tomorrow = datetime(year=now.year, month=now.month, day=now.day, hour=16, minute=0, second=0)
                x = (tomorrow - now).total_seconds()
                self.sleep_time = x
            else:
                tomorrow = now + timedelta(days=1)
                tomorrow = datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day, hour=16, minute=0, second=0)
                x = (tomorrow - now).total_seconds()
                self.sleep_time = x
        else:
            # Sleep 15 minutes try again
            self._log('not all tasks were completed, trying again in 15 minutes')
            self.sleep_time = 900
