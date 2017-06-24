from datetime import datetime, timedelta

from acquisition.symbol.tickers import StockTickers
from db.Schedule import ScheduleDB
from db.Finance import FinanceDB
from db.models.schedule import HistoricalStockData
from yfk.quote_networking import QuoteNetworking

DAYS_PER_CALL = 100

class HistoricalStockAcquisition():

    def __init__(self):
        self.task_name = 'HistoricalStockAcquisition'
        self.schedule_db = ScheduleDB()
        self.finance_db = FinanceDB('stock_historical')
        self.symbols = StockTickers().get_all()
        self.counter = 0

    def next(self):
        now = datetime.now().date()
        today = datetime(year=now.year, month=now.month, day=now.day)
        yesterday = today - timedelta(days=1)
        self.current_symbol = self.symbols[self.counter]
        stock_record = self.schedule_db.query(HistoricalStockData, {'symbol': self.current_symbol}).first()
        if stock_record is None:
            stock_record = HistoricalStockData(symbol=self.current_symbol)
            data = QuoteNetworking(symbol=self.current_symbol, start=yesterday - timedelta(DAYS_PER_CALL), end=yesterday, log_process=False).get_data()
            minute_data = filter(lambda (k,v): 'data' in v.keys() and len(v['data']) >= 390, data.iteritems())
            minute_data = dict(sorted(minute_data, key=lambda x: x[0]))
            documents = []
            start, end = None, None
            for key, data in minute_data.iteritems():
                if start is None or key.date() < start:
                    start = key.date()
                if end is None or key.date() > end:
                    end = key.date()
                data['trading_date'] = key.date()
                documents.append(data)
            stock_record.start_date = start
            stock_record.end_date = end
            stock_record.minute_start_date = start
            stock_record.has_minute_granularity = True if start and end else False
            self.finance_db.insert_many(documents)
            self.schedule_db.add_to_schedule(stock_record)
        else:
            pass


h =  HistoricalStockAcquisition()
h.next()
a = 1








