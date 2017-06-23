from datetime import datetime, timedelta

from yfk.options_networking import Options
from db import ScheduleDB, FinanceDB
from db.models.schedule import OptionTask
from logger import Logger

class OptionsAcquisition():

    def __init__(self, trading_date=None):
        self.task_name = 'OptionsAcquisition'
        self.trading_date = trading_date
        self._reset_counters()

    def _reset_counters(self):
        self.found = []
        self.not_found = []

    def _log(self, msg, level='info'):
        Logger.log(msg, level=level, threadname=self.task_name)

    def start(self):
        self._reset_counters()
        if self.trading_date.weekday() > 4:
            self._log('Not running {} on weekend'.format(self.task_name))
        elif self.trading_date.weekday() <= 4 and self.trading_date.hour < 16:
            self._log('Trading day has not finished yet, {}'.format(self.trading_date.time()))
        else:
            schedule_db = ScheduleDB()
            finance_db = FinanceDB('stock_options')
            self.symbols = schedule_db.get_incomplete_options_tasks(self.trading_date)
            o = Options(self.symbols)
            options_data = o.get_data()
            for symbol, data in options_data.iteritems():
                if 'options' in data.keys() and isinstance(data['options'], dict):
                    options = {}
                    for key in data['options'].keys():  # convert int keys into str
                        options[str(key)] = data['options'][key]
                    data['options'] = options
                    data['trading_date'] = str(self.trading_date)
                    finance_db.insert_one(data)
                    schedule_db.complete_options_task(symbol, self.trading_date)
                    self.found.append(symbol)
                else:
                    self.not_found.append(symbol)
            self._log('{}/{} found/not_found'.format(len(self.found), len(self.not_found)))
            complete = schedule_db.query(OptionTask, {'trading_date': self.trading_date.date(), 'completed': True}).all()
            incomplete = schedule_db.query(OptionTask, {'trading_date': self.trading_date.date(), 'completed': False}).all()
            self._log('{}/{} complete/incomplete'.format(len(complete), len(incomplete)))

    def sleep_time(self):
        now = datetime.now()
        if len(self.found + self.not_found) == 0:
            if now.weekday() > 4:
                next_trading = now + timedelta(days=7-now.weekday())
                tomorrow = datetime(year=next_trading.year, month=next_trading.month, day=next_trading.day, hour=16, minute=0, second=0)
                return (tomorrow - now).total_seconds()
            elif now.weekday() <= 4 and now.hour < 16:
                later = datetime(year=now.year, month=now.month, day=now.day, hour=16, minute=0, second=0)
                return (later - now).total_seconds()
            else:
                return 900
        elif len(self.found) == 0 and len(self.not_found) > 0:
            if now.hour < 16:
                later = datetime(year=now.year, month=now.month, day=now.day, hour=16, minute=0, second=0)
                return (later - now).total_seconds()
            else:
                tomorrow = now + timedelta(days=1)
                tomorrow = datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day, hour=16, minute=0, second=0)
                return (tomorrow - now).total_seconds()
        else:
            return 900

# o = OptionsAcquisition(datetime.today())
# o.start()
# a = 1




