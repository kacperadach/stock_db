import json

import requests

BASE_URL = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{}?formatted=true&crumb=cdjFLauo4.a&lang=en-US&region=US&modules=institutionOwnership%2CfundOwnership%2CmajorDirectHolders%2CmajorHoldersBreakdown%2CinsiderTransactions%2CinsiderHolders%2CnetSharePurchaseActivity&corsDomain=finance.yahoo.com"

class InsiderTransactions():

	def __init__(self, symbol):
		self.symbol = symbol
		self.url = BASE_URL.format(symbol)
		self._make_request()

	def _make_request(self):
		self.response = json.loads(requests.get(self.url).text)
		self._parse_response()

	def _parse_response(self):
		data = {}	
		print self.response['quoteSummary']['result'][0]['insiderHolders']['holders'][0]
		data['insiderHolders'] = map(lambda x: {'name': x['name'], 'relation': x['relation'], 'transaction': x['transactionDescription'], 'lastestTransDate': x['latestTransDate']['fmt'], 'positionIndirect': x['positionIndirect']['raw'], 'positionIndirectDate': x['positionIndirectDate']['fmt']}, self.response['quoteSummary']['result'][0]['insiderHolders']['holders'])
		mhb = self.response['quoteSummary']['result'][0]['majorHoldersBreakdown']
		data['majorHoldersBreakdown'] = {'insidersPercentHeld': mhb['insidersPercentHeld']['raw'], 'institutionsPercentHeld': mhb['institutionsPercentHeld']['raw'], 'institutionsFloatPercentHeld': mhb['institutionsFloatPercentHeld']['raw'], 'institutionsCount': mhb['institutionsCount']['raw']}
		self.data = data

	def get_data(self):
		return self.data
