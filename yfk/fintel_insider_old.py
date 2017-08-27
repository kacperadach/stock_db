
from bs4 import BeautifulSoup

from networking import Networking
from logger import Logger


CURRENT_OWNERSHIP = "https://fintel.io/so/us/{}"
INSIDER_TRANSACTIONS = "https://fintel.io/n/us/{}"

BATCH_SIZE = 100

class FintelAcquisition():

    def __init__(self, symbols=None, batching=False, update_percent=10):
        self.task_name = 'FintelAcquisition'
        self.symbols = symbols
        self.batching = batching
        self.last_benchmark = -1
        self.update_percent = update_percent
        self.data = {}
        if self.symbols and not batching:
            self.execute(symbols)

    def _log_process(self, batch):
        if batch/float(len(self.symbols)) * 100 > self.last_benchmark:
            self.last_benchmark += self.update_percent
            Logger.log(str(round((float(batch) / len(self.symbols)) * 100, 2)) + '%', threadname=self.task_name)

    def generate_ownership(self):
        for x in range(0, len(self.symbols), BATCH_SIZE):
            self._log_process(x)
            yield self.execute(self.symbols[x: x + BATCH_SIZE], 'ownership')
        self.last_benchmark = 0

    def generate_insider(self):
        for x in range(0, len(self.symbols), BATCH_SIZE):
            self._log_process(x)
            yield self.execute(self.symbols[x: x + BATCH_SIZE], 'insider')
        self.last_benchmark = 0

    def execute(self, symbols, mode):
        if mode.lower() == 'insider':
            base_url = INSIDER_TRANSACTIONS
            parser = self._parse_insider
        elif mode.lower() == 'ownership':
            base_url = CURRENT_OWNERSHIP
            parser = self._parse_ownership
        self._create_urls(symbols, base_url)
        self._make_requests(parser)
        return self.data

    def get_data(self):
        return self.data

    def _create_urls(self, symbols, base_url):
        self.urls = dict(map(lambda x: (x, base_url.format(x)), symbols))

    def _make_requests(self, parser):
        n = Networking(threadname=self.task_name, log_progress=not self.batching, request_method='urllib')
        from datetime import datetime
        start = datetime.now()
        data = n.execute(self.urls)
        print "Requests took: " + str(datetime.now() - start)

        start = datetime.now()
        data = dict(map(lambda (k,v): (k, parser(v)), data.iteritems()))
        print "Parsing took: " + str(datetime.now() - start)
        self.data = data

    def _parse_insider(self, response):
        bs = BeautifulSoup(response, 'html.parser')
        tables = bs.findAll('table')
        insider_table = None
        for table in tables:
            if 'id' in table.attrs.keys() and table.attrs['id'].lower() == 'transactions':
                insider_table = table
                break

        if not insider_table:
            return []

        rows = insider_table.findChildren('tr')
        if len(rows) <= 1:
            return []

        parsed_data = []
        table_headers = None
        for row in rows:
            if table_headers is None:
                table_headers = map(lambda x: x.text, row.findChildren('th'))
            else:
                table_data = map(lambda x: x.text, row.findChildren('td'))
                data = {}
                for i, header in enumerate(table_headers):
                    if header:
                        data[header] = table_data[i]
                parsed_data.append(data)
        return parsed_data

    def _parse_ownership(self, response):
        pass

if __name__ == "__main__":
    from acquisition.symbol.financial_symbols import Financial_Symbols
    fintel = FintelAcquisition(Financial_Symbols.get_all(), batching=True)
    for data in fintel.generate_insider():
        print data
    for data in fintel.generate_ownership():
        print data