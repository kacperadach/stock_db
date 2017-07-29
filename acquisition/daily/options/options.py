from datetime import datetime, timedelta

from yfk.options_networking import Options
from db import FinanceDB
from logger import Logger
from acquisition.symbol.tickers import StockTickers

class OptionsAcquisition():

    def __init__(self, trading_date=None):
        self.task_name = 'OptionsAcquisition'
        self.trading_date = trading_date
        self.finance_db = None
        self.symbols = []
        self._reset_counters()

    def _reset_counters(self):
        self.found = []
        self.not_found = []
        self.symbols = StockTickers().get_all()

    def _log(self, msg, level='info'):
        Logger.log(msg, level=level, threadname=self.task_name)

    def get_incomplete_options_tasks(self):
        symbols = []
        if not self.finance_db or not self.trading_date:
            return symbols
        found = set(map(lambda x: x['symbol'], list(self.finance_db.find({"trading_date": str(self.trading_date.date())}))))
        return list(set(self.symbols) - found)

    def get_complete_options_tasks(self):
        symbols = []
        if not self.finance_db or not self.trading_date:
            return symbols
        found = set(map(lambda x: x['symbol'], list(self.finance_db.find({"trading_date": str(self.trading_date.date())}))))
        return list(found)

    def start(self):
        self._reset_counters()
        if self.trading_date.weekday() > 4:
            self._log('Not running {} on weekend'.format(self.task_name))
        elif self.trading_date.weekday() <= 4 and self.trading_date.hour < 16:
            self._log('Trading day has not finished yet, {}'.format(self.trading_date.time()))
        else:
            self.finance_db = FinanceDB('stock_options')
            incomplete = self.get_incomplete_options_tasks()
            o = Options(incomplete)
            options_data = o.get_data()
            for symbol, data in options_data.iteritems():
                if 'options' in data.keys() and isinstance(data['options'], dict):
                    options = {}
                    for key in data['options'].keys():  # convert int keys into str
                        options[str(key)] = data['options'][key]
                    data['symbol'] = data['underlyingSymbol']
                    data.pop('underlyingSymbol', None)
                    data['options'] = options
                    data['trading_date'] = str(self.trading_date.date())
                    self.finance_db.insert_one(data)
                    self.found.append(symbol)
                else:
                    self.not_found.append(symbol)

            self._log('{}/{} found/not_found'.format(len(self.found), len(self.not_found)))
            incomplete = self.get_incomplete_options_tasks()
            complete = self.get_complete_options_tasks()
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

if __name__ == "__main__":
    from datetime import datetime
    OptionsAcquisition(datetime.now()).start()