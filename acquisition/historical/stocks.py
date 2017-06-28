from datetime import datetime, timedelta

from acquisition.symbol.tickers import StockTickers
from db.Schedule import ScheduleDB
from db.Finance import FinanceDB
from db.models.schedule import HistoricalStockData
from yfk.quote_networking import QuoteNetworking
from logger import Logger

DAYS_PER_CALL = 100
LOG_PERCENT = 5
REQUESTS_PER_ITERATION = 100

class HistoricalStockAcquisition():

    def __init__(self):
        self.task_name = 'HistoricalStockAcquisition'
        self.schedule_db = ScheduleDB()
        self.finance_db = FinanceDB('stock_historical')
        self.symbols = StockTickers().get_all()
        self.counter = 0
        self.date = None
        self.last_benchmark = 0

    def _log(self, msg, level='info'):
        Logger.log(msg, level=level, threadname=self.task_name)

    def _log_process(self):
        progress = (float(self.counter) / len(self.symbols)) * 100
        if  progress > self.last_benchmark:
            self._log(str(round(float(progress), 2)) + '%')
            self.last_benchmark += LOG_PERCENT

    def next(self):
        self._log_process()
        if self.date is None:
            self.date = datetime.now().date()
        now = datetime.now().date()
        yesterday = datetime(year=now.year, month=now.month, day=now.day) - timedelta(days=1)
        if self.counter < len(self.symbols):
            self.current_symbol = self.symbols[self.counter]
        else:
            self.counter = 0
            self.last_benchmark = 0
            raise StopIteration
        stock_record = self.schedule_db.query(HistoricalStockData, {'symbol': self.current_symbol}).first()

        #symbol_dates = []
        #stock_recods = []
        #while sum(map(lambda x: x['requests'], symbol_dates)) < 50:
        if stock_record is None:
            start = yesterday - timedelta(days=DAYS_PER_CALL)
            end = yesterday
        else:
            end_date = datetime(year=stock_record.end_date.year, month=stock_record.end_date.month, day=stock_record.end_date.day)
            start = end_date + timedelta(days=1)
            end = yesterday
        data = QuoteNetworking(symbol=self.current_symbol, start=start, end=end, log_process=False).get_data()
        minute_data = filter(lambda (k, v): 'data' in v.keys() and len(v['data']) >= 390, data.iteritems())
        minute_data = dict(sorted(minute_data, key=lambda x: x[0]))
        documents = []

        start,end = None, None
        for key, data in minute_data.iteritems():
            if start is None or key.date() < start:
                start = key.date()
            if end is None or key.date() > end:
                end = key.date()
            data['trading_date'] = str(key.date())
            data['symbol'] = self.current_symbol
            documents.append(data)

        if stock_record is None:
            stock_record = HistoricalStockData(symbol=self.current_symbol)
            stock_record.start_date = start
            stock_record.minute_start_date = start
            stock_record.has_minute_granularity = True if start and end else False

        if end and documents:
            stock_record.end_date = end
            self.finance_db.insert_many(documents)
            self.schedule_db.add_to_schedule(stock_record)

        self.counter += 1
