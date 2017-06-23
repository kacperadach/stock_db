from datetime import date, datetime, timedelta

from db.models.schedule import OptionTask, CommodityTask, InsiderTask
from db.Schedule import ScheduleDB
from logger import Logger
from app.thread import FThread
from acquisition.daily.options import OptionsAcquisition
from acquisition.daily.insider import InsiderAcquisition
from acquisition.daily.commodities import CommoditiesAcquisition


class Acquirer(FThread):

    def __init__(self):
        super(Acquirer, self).__init__()
        self.thread_name = 'Acquirer'
        self.trading_day = None
        self.tasks = [OptionsAcquisition(), InsiderAcquisition(), CommoditiesAcquisition()]

    def _run(self):
        self.trading_date = datetime.now()
        for task in self.tasks:
            self._log('beginning {} task'.format(task.task_name))
            start = datetime.now()
            task.trading_date = self.trading_date
            task.start()
            end = datetime.now()
            self._log('{} task took {}'.format(task.task_name, end - start))

    # def complete_task(self, task_fn):
    #     self._log('beginning {} task'.format(task_fn.func_name))
    #     self.history[task_fn.func_name] = False
    #     start = datetime.now()
    #     found, not_found = task_fn(self.trading_date)
    #     end = datetime.now()
    #     self._log('{} task took {}'.format(task_fn.func_name, end - start))
    #     return found, not_found

    # def assert_complete(self, task_fn, found, not_found):
    #     s = ScheduleDB()
    #     table = None
    #     if task_fn == get_all_options_data:
    #         table = OptionTask
    #     elif task_fn == get_all_insider_data:
    #         table = InsiderTask
    #     elif task_fn == get_all_commodities_data:
    #         table = CommodityTask
    #
    #     if table:
    #         if table == CommodityTask:
    #             trading_date = self.trading_date - timedelta(days=1)
    #         else:
    #             trading_date = self.trading_date
    #         complete = s.query(table, {'trading_date': trading_date, 'completed': True}).all()
    #         incomplete = s.query(table, {'trading_date': trading_date, 'completed': False}).all()
    #         self._log('{} / {}  found/not_found'.format(len(found), len(not_found)))
    #         self._log('{} / {}  complete/incomplete'.format(len(complete), len(incomplete)))
    #         if len(found) == 0 and len(not_found) < len(complete):
    #             self._log('Completed {}'.format(task_fn.func_name))
    #             self.history[task_fn.func_name] = True
    #         else:
    #             self._log('{} was not fully completed'.format(task_fn.func_name))
    #     else:
    #         self._log('Task not found in assert_complete', level='error')

    def _sleep(self):
        self.sleep_time = min(map(lambda x: x.sleep_time(), self.tasks))
