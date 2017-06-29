from datetime import datetime, timedelta, date

from yfk.quote import Quote, QuoteError
from db import ScheduleDB, FinanceDB
from db.models.schedule import CommodityTask
from logger import Logger

def get_commodities_data(symbol, trading_date):
    yesterday = trading_date - timedelta(days=1)
    period1 = datetime(year=yesterday.year, month=yesterday.month, day=yesterday.day, hour=0, minute=0)
    period2 = datetime(year=trading_date.year, month=trading_date.month, day=trading_date.day, hour=0, minute=0)
    try:
        it = Quote(symbol, period1=period1, period2=period2, interval='1m', auto_query=True)
        return it.get_data()
    except QuoteError:
        return {}

class CommoditiesAcquisition():

    def __init__(self, trading_date=None):
        self.task_name = 'CommoditiesAcquisition'
        self.trading_date = trading_date
        self._reset_counters()

    def _reset_counters(self):
        self.found = []
        self.not_found = []

    def _log(self, msg, level='info'):
        Logger.log(msg, level=level, threadname=self.task_name)

    def start(self):
        self._reset_counters()
        schedule_db = ScheduleDB()
        finance_db = FinanceDB('commodities')
        yesterday = self.trading_date - timedelta(days=1)
        yesterday = datetime(year=yesterday.year, month=yesterday.month, day=yesterday.day)
        for symbol in Logger.progress(schedule_db.get_incomplete_commodities_tasks(yesterday), 'Commodities'):
            data = get_commodities_data(symbol, self.trading_date)
            if data:
                # trading date for commodities is the day before
                data['trading_date'] = str(yesterday.date())
                finance_db.insert_one(data)
                schedule_db.complete_commodities_task(symbol, yesterday)
                self.found.append(symbol)
            else:
                self.not_found.append(symbol)
        self._log('{}/{} found/not_found'.format(len(self.found), len(self.not_found)))
        self.complete = schedule_db.query(CommodityTask, {'trading_date': yesterday.date(), 'completed': True}).all()
        self.incomplete = schedule_db.query(CommodityTask, {'trading_date': yesterday.date(), 'completed': False}).all()
        self._log('{}/{} complete/incomplete'.format(len(self.complete), len(self.incomplete)))

    def sleep_time(self):
        now = datetime.now()
        if len(self.complete + self.incomplete) == 0:
            return 900
        elif len(self.found) == 0 and len(self.not_found) > 0:
            tomorrow = now + timedelta(days=1)
            tomorrow = datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day, hour=16, minute=0, second=0)
            return (tomorrow - now).total_seconds()
        else:
            return 900
