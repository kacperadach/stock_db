from datetime import datetime, timedelta

from acquisition.symbol.financial_symbols import Financial_Symbols
from db.Finance import FinanceDB
from yfk.quote_networking import QuoteNetworking
from logger import Logger
from discord.webhook import DiscordWebhook

DAYS_PER_CALL = 50
LOG_PERCENT = 5
REQUESTS_PER_ITERATION = 150

class HistoricalStockAcquisition():

    def __init__(self):
        self.task_name = 'HistoricalStockAcquisition'
        self.finance_db = FinanceDB('stock_historical')
        self.discord = DiscordWebhook()
        self.symbols = Financial_Symbols.get_all()
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
        if self.counter == 0:
            self._log('Beginning {}'.format(self.task_name))
        start_time = datetime.now()
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
            stock_documents = list(self.finance_db.find({"symbol": self.current_symbol}, {"symbol": 1, "trading_date": 1}))
            if len(stock_documents) == 0:
                start_date = yesterday - timedelta(days=DAYS_PER_CALL)
                end_date = yesterday
            else:
                start = datetime.strptime(min(map(lambda x: x['trading_date'], stock_documents)), "%Y-%m-%d")
                end = datetime.strptime(max(map(lambda x: x['trading_date'], stock_documents)), "%Y-%m-%d")
                if end < yesterday:
                    start_date = end
                    end_date = yesterday
                elif (end - start).days < DAYS_PER_CALL:
                    start_date = end - timedelta(days=DAYS_PER_CALL)
                    end_date = start
                else:
                    self.counter += 1
                    continue

            symbol_dates.append({
                'symbol': self.current_symbol,
                'start': start_date,
                'end': end_date
            })
            requests += (end_date-start_date).days + 1
            self.counter += 1

        data = QuoteNetworking(symbols=symbol_dates, log_process=False).get_data()
        minute_data = dict(filter(lambda (k, v): 'data' in v.keys() and len(v['data']) > 100, data.iteritems()))
        documents = []

        for key, data in minute_data.iteritems():
            try:
                dt = key.split('-', key.count('-') - 2)
                dt = dt[len(dt)-1]
                dt = datetime.strptime(dt, '%Y-%m-%d').date()

                symbol = key.split('-', key.count('-') - 2)[0]
                for sym_date in symbol_dates:
                    if sym_date['symbol'] == symbol:
                        if 'data_start' not in sym_date.keys() or sym_date['data_start'] > dt:
                            sym_date['data_start'] = dt
                        if 'data_end' not in sym_date.keys() or sym_date['data_end'] < dt:
                            sym_date['data_end'] = dt
                        break
                data['trading_date'] = str(dt)
                data['symbol'] = symbol
                data['start_time'] = min(map(lambda x: x[0],data['data']))
                data['end_time'] = max(map(lambda x: x[0],data['data']))
                documents.append(data)
            except Exception as e:
                self._log("Unexpected Error occurred: {}".format(str(e)))
                if Logger.env.lower() == 'prod':
                    self.discord.alert_error(self.task_name, str(e))

        if documents:
            self.finance_db.insert_many(documents)

        self._log('Time took: {}'.format(datetime.now()-start_time))


if __name__ == "__main__":
    h = HistoricalStockAcquisition()
    while 1:
        h.next()