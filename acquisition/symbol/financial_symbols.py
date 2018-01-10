from urllib2 import Request, urlopen, URLError
from re import findall
from datetime import datetime
from random import shuffle

from commodities import Commodities_Symbols
from db.Finance import Finance_DB
from etfs import ETF
from logger import Logger

IGNORED_STRINGS = ('SYMBOL', )
FILTERED_SYMBOLS = ('^', '.', '$')
EXCHANGES = ('nasdaq', 'nyse', 'amex')
API_URL = 'http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange={}&render=download'
CHECK_API_TIME_INTERVAL = 14400

SYMBOLS_COLLECTION = 'symbols'

class FinancialSymbols():

	def __init__(self):
		self.task_name = 'FinancialSymbols'
		self.all_symbols = []
		self.last_check = None
		self.finance_db = Finance_DB
		self.ETF = ETF()
		self._get_symbols()

	def _log(self, msg, level='info'):
		Logger.log(msg, level, self.task_name)

	def _get_symbols(self):
		self._log('Fetching symbols')
		self._write_symbols_to_mongo(self._fetch_symbols(), 'Symbols')
		self._write_symbols_to_mongo(self._fetch_etfs(), 'ETFs')
		self.all_symbols = self._read_symbols_from_mongo()
		self.last_check = datetime.now()

	def _read_symbols_from_mongo(self):
		return list(set(map(lambda x: x['symbol'], self.finance_db.find(SYMBOLS_COLLECTION, {}, {"symbol": 1}))))

	def _write_symbols_to_mongo(self, symbols, symbol_type):
		documents = []
		symbols = set(symbols)
		all_symbols = set(map(lambda x: x['symbol'], list(self.finance_db.find(SYMBOLS_COLLECTION, {}, {"symbol": 1}))))
		new_symbols = symbols - all_symbols
		if new_symbols:
			self._log('{} new {} found'.format(len(new_symbols), symbol_type))
		for symbol in new_symbols:
			documents.append({"symbol": symbol, "created_on": str(datetime.now().date())})
		if documents:
			self.finance_db.insert(SYMBOLS_COLLECTION, documents)

	def _fetch_symbols(self):
		all_symbols = []
		for exchange in EXCHANGES:
			try:
				response = urlopen(Request(API_URL.format(exchange)))
				ticker_list = self._extract_tickers_from_api_response(response.read())
				all_symbols += ticker_list
			except URLError:
				pass
		all_symbols = filter(lambda x: bool(x), self._filter_all_tickers(map(lambda x: x.split(",")[0].replace('"', '').strip(), all_symbols)))
		new_symbols = set(all_symbols) - set(self.all_symbols)
		return new_symbols

	def _fetch_etfs(self):
		return self.ETF.get_all_etfs()

	def _extract_tickers_from_api_response(self, ticker_list):
		all_tickers = []
		ticker_list = ticker_list.split("\n")
		for ticker in ticker_list:
			try:
				ticker_string = findall('"([^"]*)"', ticker)[0]
				if not ticker_string.lower() == 'symbol':
					all_tickers.append(ticker_string)
			except IndexError:
				continue
		return ticker_list

	def _filter_all_tickers(self, all_tickers):
		filtered_list = []
		for ticker in all_tickers:
			if not any(sym in ticker for sym in FILTERED_SYMBOLS) and ticker.upper() not in IGNORED_STRINGS:
				filtered_list.append(ticker)
		filtered_list.sort()
		return filtered_list

	def get_all(self):
		if self.last_check is None or (datetime.now() - self.last_check).total_seconds() > CHECK_API_TIME_INTERVAL:
			self._get_symbols()
		shuffle(self.all_symbols)
		return self.all_symbols

	def get_commodities(self):
		return Commodities_Symbols

Financial_Symbols = FinancialSymbols()

if __name__ == "__main__":
	print FinancialSymbols().get_all()
