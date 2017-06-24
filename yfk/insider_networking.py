from networking import Networking

BASE_URL = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{}?formatted=true&crumb=cdjFLauo4.a&lang=en-US&region=US&modules=institutionOwnership%2CfundOwnership%2CmajorDirectHolders%2CmajorHoldersBreakdown%2CinsiderTransactions%2CinsiderHolders%2CnetSharePurchaseActivity&corsDomain=finance.yahoo.com"

class InsiderTransactions():

    def __init__(self, symbols):
        self.symbol = symbols
        self.data = {}
        self.urls = dict(map(lambda x: (x, BASE_URL.format(x)), symbols))
        self._make_requests()

    def _make_requests(self):
        n = Networking(threadname='InsiderAcquisition')
        data = n.execute(self.urls)
        data = dict(map(lambda (k,v): (k, v['quoteSummary']['result'][0] if 'quoteSummary' in v.keys() and 'result' in v['quoteSummary'].keys() and v['quoteSummary']['result'] and len(v['quoteSummary']['result']) > 0 else {}), data.iteritems()))
        self.data = data

    def get_data(self):
        return self.data
