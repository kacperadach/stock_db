from datetime import date, datetime, timedelta

from db.models.schedule import OptionTask, CommodityTask, InsiderTask
from db.Schedule import ScheduleDB
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

        self.trading_date = self.trading_date if not hasattr(self.trading_date, 'date') else self.trading_date.date()
        found, not_found = self.complete_task(get_all_commodities_data)
        self.assert_complete(get_all_commodities_data, found, not_found)

    def complete_task(self, task_fn):
        self._log('beginning {} task'.format(task_fn.func_name))
        self.history[task_fn.func_name] = False
        start = datetime.now()
        found, not_found = task_fn(self.trading_date)
        end = datetime.now()
        self._log('{} task took {}'.format(task_fn.func_name, end - start))
        return found, not_found

    def assert_complete(self, task_fn, found, not_found):
        s = ScheduleDB()
        table = None
        if task_fn == get_all_options_data:
            table = OptionTask
        elif task_fn == get_all_insider_data:
            table = InsiderTask
        elif task_fn == get_all_commodities_data:
            table = CommodityTask

        if table:
            if table == CommodityTask:
                trading_date = self.trading_date - timedelta(days=1)
            else:
                trading_date = self.trading_date
            complete = s.query(table, {'trading_date': trading_date, 'completed': True}).all()
            incomplete = s.query(table, {'trading_date': trading_date, 'completed': False}).all()
            self._log('{} / {}  found/not_found'.format(len(found), len(not_found)))
            self._log('{} / {}  complete/incomplete'.format(len(complete), len(incomplete)))
            if (len(found) == 0 and len(not_found) < len(complete)) or len(incomplete) == 0:
                self._log('Completed {}'.format(task_fn.func_name))
                self.history[task_fn.func_name] = True
            else:
                self._log('{} was not fully completed'.format(task_fn.func_name))
        else:
            self._log('Task not found in assert_complete', level='error')

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
