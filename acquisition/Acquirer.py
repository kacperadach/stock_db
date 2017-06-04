from datetime import date, datetime, timedelta

from logger import Logger
from app.thread import FThread
from acquisition.daily.options import get_all_options_data
from acquisition.daily.insider import get_all_insider_data


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
            Logger.log('{}: Not running Acquirer on weekend: {}'.format(self.thread_name, self.trading_date))
        else:
            if self.trading_date.hour < 16:
                Logger.log('{}: Trading day has not finished yet, {}'.format(self.thread_name, self.trading_date.time()))
            else:
                self.trading_date = self.trading_date.date()
                Logger.log('{}: using {} as trading day'.format(self.thread_name, self.trading_date))

                found, not_found = self.complete_task(get_all_options_data)
                self.assert_complete(get_all_options_data, found, not_found)

                found, not_found = self.complete_task(get_all_insider_data)
                self.assert_complete(get_all_insider_data, found, not_found)

    def complete_task(self, task_fn):
        Logger.log('{}: beginning {} task'.format(self.thread_name, task_fn.func_name))
        self.history[task_fn.func_name] = False
        start = datetime.now()
        found, not_found = task_fn(self.trading_date)
        end = datetime.now()
        Logger.log('{}: {} task took {}'.format(self.thread_name, task_fn.func_name, end - start))
        Logger.log('{}: acquired data for {} securities.'.format(self.thread_name, len(found)))
        return found, not_found

    def assert_complete(self, task_fn, found, not_found):
        if len(found) + len(not_found) == 0:
            Logger.log('{}: no securities scheduled while completing task {}'.format(self.thread_name, task_fn.func_name), 'warning')
        elif len(not_found) > 50:
            Logger.log('{}: could not find data for {} securities while completing task {}'.format(self.thread_name, len(not_found), task_fn.func_name), 'warning')
        else:
            self.history[task_fn.func_name] = True

    def _sleep(self):

        now = datetime.now()
        if all(self.history.values()) or (now.hour == 23 and now.minute >= 45):
            # Sleep until next day
            tomorrow = now + timedelta(days=1)
            tomorrow = datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day, hour=16, minute=0, second=0)
            x = (tomorrow - now).total_seconds()
            self.sleep_time = x
        else:
            # Sleep 15 minutes try again
            Logger.log('{}: not all tasks were completed, trying again in 15 minutes'.format(self.thread_name))
            self.sleep_time = 900