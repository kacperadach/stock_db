from datetime import datetime, timedelta

from acquisition.symbol.tickers import StockTickers
from db.Schedule import ScheduleDB
from db.Finance import FinanceDB
from db.models.schedule import HistoricalStockData
from yfk.quote_networking import QuoteNetworking
from logger import Logger

DAYS_PER_CALL = 50
LOG_PERCENT = 5
REQUESTS_PER_ITERATION = 150

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
        if len(self.symbols) > 0:
            progress = (float(self.counter) / len(self.symbols)) * 100
            if  progress > self.last_benchmark:
                self._log(str(round(float(progress), 2)) + '%')
                self.last_benchmark += LOG_PERCENT

    def next(self):
        self._log_process()
        if self.date is None:
            self.date = datetime.now().date()

        if self.counter >= len(self.symbols):
            self.counter = 0
            self.last_benchmark = 0
            raise StopIteration

        now = datetime.now().date()
        yesterday = datetime(year=now.year, month=now.month, day=now.day) - timedelta(days=1)

        symbol_dates = []
        requests = 0
        while requests < REQUESTS_PER_ITERATION and self.counter < len(self.symbols):
            self.current_symbol = self.symbols[self.counter]
            stock_record = self.schedule_db.query(HistoricalStockData, {'symbol': self.current_symbol}).first()
            if stock_record is None:
                stock_record = HistoricalStockData(symbol=self.current_symbol)
                start = yesterday - timedelta(days=DAYS_PER_CALL)
                end = yesterday
            else:
                end_date = datetime(year=stock_record.end_date.year, month=stock_record.end_date.month, day=stock_record.end_date.day)
                start = end_date + timedelta(days=1)
                end = yesterday

            if end > start:
                symbol_dates.append({
                    'symbol': self.current_symbol,
                    'start': start,
                    'end': end,
                    'stock_record': stock_record
                })
                requests += (end-start).days + 1
            self.counter += 1

        data = QuoteNetworking(symbols=symbol_dates, log_process=False).get_data()
        minute_data = dict(filter(lambda (k, v): 'data' in v.keys() and len(v['data']) > 100, data.iteritems()))
        documents = []

        for key, data in minute_data.iteritems():
            symbol, dt = key.split('-', 1)
            dt = datetime.strptime(dt, '%Y-%m-%d').date()
            for sym_date in symbol_dates:
                if sym_date['symbol'] == symbol:
                    if 'data_start' not in sym_date.keys() or sym_date['data_start'] > dt:
                        sym_date['data_start'] = dt
                    if 'data_end' not in sym_date.keys() or sym_date['data_end'] < dt:
                        sym_date['data_end'] = dt
                    break
            data['trading_date'] = str(dt)
            data['symbol'] = symbol
            documents.append(data)

        records = []
        for sym_date in symbol_dates:
            if 'data_end' not in sym_date.keys() and 'data_start' not in sym_date.keys():
                continue
            else:
                stock_record = sym_date['stock_record']
                if stock_record.start_date is None:
                    stock_record.has_minute_granularity = True
                    stock_record.start_date = sym_date['data_start']
                    stock_record.minute_start_date = sym_date['data_start']

                stock_record.end_date = sym_date['data_end']
                records.append(stock_record)

        if records and documents:
            self.finance_db.insert_many(documents)
            self.schedule_db.add_to_schedule(records)
