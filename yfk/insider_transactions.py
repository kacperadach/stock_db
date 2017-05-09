import requests_cache
import requests

BASE_URL = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{}?formatted=true&crumb=cdjFLauo4.a&lang=en-US&region=US&modules=institutionOwnership%2CfundOwnership%2CmajorDirectHolders%2CmajorHoldersBreakdown%2CinsiderTransactions%2CinsiderHolders%2CnetSharePurchaseActivity&corsDomain=finance.yahoo.com"

requests_cache.install_cache('insider_transcation_cache')

class InsiderTransactions():

	def __init__(self, symbol):
		self.symbol = symbol
		self.url = BASE_URL.format(symbol)
		self._make_request()

	def _make_request(self):
		self.response = requests.get(self.url)
		self._parse_response()

	def _parse_response(self):
		data = {}
		data['insiderHolders'] = map(, self.response['quoteSummary']['result'][0]['insiderHolders'])

	def get_data(self):
		if not self.data:
			pass
		return self.data


