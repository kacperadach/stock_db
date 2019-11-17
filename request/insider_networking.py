from networking import Networking


BASE_URL = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{}?formatted=true&crumb=cdjFLauo4.a&lang=en-US&region=US&modules=institutionOwnership%2CfundOwnership%2CmajorDirectHolders%2CmajorHoldersBreakdown%2CinsiderTransactions%2CinsiderHolders%2CnetSharePurchaseActivity&corsDomain=finance.yahoo.com"

BATCH_SIZE = 100

class InsiderTransactions():

    def __init__(self, symbols=None, batching=False, update_percent=10):
        self.task_name = 'InsiderAcquisition'
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

    def generate(self):
        for x in range(0, len(self.symbols), BATCH_SIZE):
            self._log_process(x)
            yield self.execute(self.symbols[x: x + BATCH_SIZE])
        self.last_benchmark = 0

    def execute(self, symbols):
        self._create_urls(symbols)
        self._make_requests()
        return self.data

    def get_data(self):
        return self.data

    def _create_urls(self, symbols):
        self.urls = dict(list(map(lambda x: (x, BASE_URL.format(x)), symbols)))

    def _make_requests(self):
        n = Networking(threadname=self.task_name, log_progress=not self.batching)
        data = n.execute(self.urls)
        data = dict(list(map(lambda k,v: (k, v['quoteSummary']['result'][0] if 'quoteSummary' in v.keys() and 'result' in v['quoteSummary'].keys() and v['quoteSummary']['result'] and len(v['quoteSummary']['result']) > 0 else {}), data.items())))
        self.data = data


