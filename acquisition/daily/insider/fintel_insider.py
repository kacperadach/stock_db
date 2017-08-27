from datetime import datetime, timedelta

from yfk.insider_networking import InsiderTransactions
from db import FinanceDB
from logger import Logger
from acquisition.symbol.financial_symbols import Financial_Symbols


class FintelInsiderAcquisition():

    def __init__(self, trading_date=None):
        self.task_name = 'FintelInsiderAcquisition'
        self.trading_date = trading_date
        self.symbols = Financial_Symbols.get_all()
        self.finance_db = None
        self._reset_counters()

    def _reset_counters(self):
        self.found = 0
        self.not_found = 0
        self.symbols = Financial_Symbols.get_all()

    def _log(self, msg, level='info'):
        Logger.log(msg, level=level, threadname=self.task_name)

    def get_incomplete_insider_tasks(self):
        if not self.finance_db or not self.trading_date:
            return []
        found = set(list(map(lambda x: x['symbol'], self.finance_db.find({"trading_date": str(self.trading_date.date())}, {"symbol": 1}))))
        return list(set(self.symbols) - found)

    def get_complete_insider_tasks(self):
        symbols = []
        if not self.finance_db or not self.trading_date:
            return symbols
        found = set(map(lambda x: x['symbol'], list(self.finance_db.find({"trading_date": str(self.trading_date.date())}, {"symbol": 1}))))
        return list(found)

    def start(self):
        self._reset_counters()
        if self.trading_date.weekday() > 4:
            self._log('Not running {} on weekend'.format(self.task_name))
        elif self.trading_date.weekday() <= 4 and self.trading_date.hour < 16:
            self._log('Trading day has not finished yet, {}'.format(self.trading_date.time()))
        else:
            self.finance_db = FinanceDB('stock_insider')
            incomplete = self.get_incomplete_insider_tasks()
            insider_transactions = InsiderTransactions(incomplete, batching=True)

            for insider_data in insider_transactions.generate():
                documents = []
                for symbol, data in insider_data.iteritems():
                    if data:
                        data['trading_date'] = str(self.trading_date.date())
                        data['symbol'] = symbol
                        documents.append(data)
                        self.found += 1
                    else:
                        self.not_found += 1
                if documents:
                    self.finance_db.insert_many(documents)

            self._log('{}/{} found/not_found'.format(self.found, self.not_found))
            # incomplete = len(self.get_incomplete_insider_tasks())
            # complete = len(self.get_complete_insider_tasks())
            # self._log('{}/{} complete/incomplete'.format(complete, incomplete))

    def sleep_time(self):
        now = datetime.now()
        if self.found + self.not_found == 0:
            if now.weekday() > 4:
                next_trading = now + timedelta(days=7-now.weekday())
                tomorrow = datetime(year=next_trading.year, month=next_trading.month, day=next_trading.day, hour=16, minute=0, second=0)
                return (tomorrow - now).total_seconds()
            elif now.weekday() <= 4 and now.hour < 16:
                later = datetime(year=now.year, month=now.month, day=now.day, hour=16, minute=0, second=0)
                return (later - now).total_seconds()
            else:
                return 900
        elif self.found == 0 and self.not_found > 0:
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
    FintelInsiderAcquisition(datetime.now()).start()