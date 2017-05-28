import json
import requests

BASE_URL = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{}?formatted=true&crumb=cdjFLauo4.a&lang=en-US&region=US&modules=institutionOwnership%2CfundOwnership%2CmajorDirectHolders%2CmajorHoldersBreakdown%2CinsiderTransactions%2CinsiderHolders%2CnetSharePurchaseActivity&corsDomain=finance.yahoo.com"

class InsiderError(Exception):
	pass

class InsiderTransactions():

	def __init__(self, symbol):
		self.symbol = symbol
		self.url = BASE_URL.format(symbol)
		self._make_request()

	def _make_request(self):
		self.response = json.loads(requests.get(self.url).text)
		if self._validate_response():
			self._parse_response()
		else:
			raise InsiderError('Insider information not found for {}'.format(self.symbol))

	def _validate_response(self):
		if 'quoteSummary' in self.response.keys():
			if isinstance(self.response['quoteSummary'], dict) and 'result' in self.response['quoteSummary'].keys():
				if isinstance(self.response['quoteSummary']['result'], list) and len(self.response['quoteSummary']['result']) >= 1:
					return True
		return False

	def _parse_response(self):
		self.data = self.response['quoteSummary']['result'][0]

	def get_data(self):
		return self.data
