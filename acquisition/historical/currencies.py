from datetime import datetime, timedelta

from acquisition.symbol.currencies import Currency_Pairs
from db.Finance import FinanceDB
from request.quote import Quote, QuoteResponse
from request.networking import Networking
from logger import Logger

DAYS_PER_CALL = 50
LOG_PERCENT = 5

class HistoricalCurrenciesAcquisition():

    def __init__(self):
        self.task_name = 'Historical Currencies Acquisition'
        self.finance_db = FinanceDB('currencies')
        self.symbols = Currency_Pairs
        self.counter = 0
        self.current_symbol = self.symbols[self.counter]
        self.current_date = None
        self.last_benchmark = 0

    def _log(self, msg, level='info'):
        Logger.log(msg, level=level, threadname=self.task_name)

    def _log_process(self):
        if len(self.symbols) > 0:
            progress = (float(self.counter) / len(self.symbols)) * 100
            if  progress > self.last_benchmark:
                self._log(str(round(float(progress), 2)) + '%')
                self.last_benchmark += LOG_PERCENT

    def next(self):
        if self.counter == 0:
            self._log('Beginning {}'.format(self.task_name))
        self._log_process()

        if self.counter >= len(self.symbols):
            self.counter = 0
            self.last_benchmark = 0
            raise StopIteration

        now = datetime.now().date()
        yesterday = datetime(year=now.year, month=now.month, day=now.day) - timedelta(days=1)

        self.current_symbol = self.symbols[self.counter]

        symbol_dates = []
        if self.current_date is None:
            self.current_date = yesterday

        start = self.current_date
        while self.current_date > start - timedelta(days=DAYS_PER_CALL):
            skip = False
            data_generator = self.finance_db.find({"meta.symbol": self.current_symbol, "trading_date": { "$lte": self.current_date.strftime("%Y-%m-%d")}})
            for data in data_generator:
                trading_date = datetime.strptime(data['trading_date'], '%Y-%m-%d')
                if trading_date == self.current_date:
                    skip = True
                    break
            if not skip:
                q = Quote(self.current_symbol, period1=self.current_date, period2=self.current_date+timedelta(days=1), interval='1m', auto_query=False).url
                symbol_dates.append((self.current_date.date(), q))
            self.current_date = self.current_date - timedelta(days=1)

        print 'Excuting {} urls for {} ending at {}'.format(len(symbol_dates), self.current_symbol, self.current_date.date())
        n = Networking(log_progress=False)
        data = n.execute(dict(symbol_dates))
        data = dict(map(lambda (k, v): (k, QuoteResponse(v).get_data()), data.iteritems()))

        if not any(map(lambda x: bool(x[1]), data.items()[0:10])) or len(symbol_dates) < 10:
            self.current_date = None
            self.counter += 1

        documents = []
        for date, d in data.iteritems():
            if d:
                d['trading_date'] = str(date)
                if 'meta' in d.keys() and isinstance(d['meta'], dict) and 'symbol' in d['meta'].keys():
                    d['symbol'] = d['meta']['symbol']
                else:
                    continue
                documents.append(d)

        if documents:
            self.finance_db.insert_many(documents)

if __name__ == "__main__":
    currencies = HistoricalCurrenciesAcquisition()
    while 1:
        currencies.next()