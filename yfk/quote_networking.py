from datetime import timedelta

from networking import Networking
from quote import Quote, QuoteResponse

class QuoteNetworking():

    def __init__(self, symbol, start, end, task_name='', log_process=True, interval='1m'):
        self.interval = interval
        self.symbol = symbol
        self.task_name = task_name
        self.log_progress = log_process
        self.start = start
        self.end = end
        self._make_requests()

    def _make_requests(self):
        current_date = self.start
        urls = []
        while current_date <= self.end:
            urls.append((current_date, Quote(symbol=self.symbol, period1=current_date, period2=current_date+timedelta(days=1), interval=self.interval).url))
            current_date = current_date + timedelta(days=1)
        n = Networking(threadname=self.task_name, log_progress=self.log_progress)
        data = n.execute(dict(urls))
        data = dict(map(lambda (k,v): (k, QuoteResponse(v).get_data()), data.iteritems()))
        self.data = data

    def get_data(self):
        return self.data
