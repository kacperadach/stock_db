from datetime import timedelta

from networking import Networking
from quote import Quote, QuoteResponse

class QuoteNetworking():

    def __init__(self, symbols, task_name='', log_process=True, interval='1m'):
        self.interval = interval
        self.symbols = symbols
        self.task_name = task_name
        self.log_progress = log_process
        self._make_requests()

    def _make_requests(self):
        urls = []
        for symbol in self.symbols:
            current_date = symbol['start']
            while current_date <= symbol['end']:
                urls.append((symbol['symbol'] + '-' + str(current_date.date()), Quote(symbol=symbol['symbol'], period1=current_date, period2=current_date+timedelta(days=1), interval=self.interval).url))
                current_date = current_date + timedelta(days=1)
        n = Networking(threadname=self.task_name, log_progress=self.log_progress)
        data = n.execute(dict(urls))
        data = dict(map(lambda (k,v): (k, QuoteResponse(v).get_data()), data.iteritems()))
        self.data = data

    def get_data(self):
        return self.data
