from os import path
from urllib2 import Request, urlopen, URLError
from re import findall
from datetime import datetime

from commodities import Commodities_Symbols
from db import FinanceDB

SPECIFIC_PATH = 'all_tickers.txt'
DIRNAME, _ = path.split(path.abspath(__file__))
BASE_PATH = path.dirname(path.abspath(__file__))

FTP_ADDRESS = 'ftp.nasdaqtrader.com'
FTP_CWD = 'SymbolDirectory'
NASDAQ_FILE = 'nasdaqlisted.txt'
OTHER_FILE = 'otherlisted.txt'
FTP_DELIMITER = '|'
IGNORED_STRINGS = ('SYMBOL', )
FILTERED_SYMBOLS = ('^', '.', '$')
FILES = ["amex.txt", "nasdaq.txt", "nyse.txt"]
TICKERS_FOLDER = 'tickers'

EXCHANGES = ('nasdaq', 'nyse', 'amex')
API_URL = 'http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange={}&render=download'

CHECK_API_TIME_INTERVAL = 14400

class FinancialSymbols():

	def __init__(self):
		self.all_symbols = []
		self.last_check = None
		self.finance_db = FinanceDB('symbols')
		self._get_symbols()

	def _get_symbols(self):
		symbols = self._make_requests()
		self._write_symbols_to_mongo(symbols)

	def _write_symbols_to_mongo(self, symbols):
		documents = []
		all_symbols = set(map(lambda x: x['symbol'], list(self.finance_db.find({}))))
		new_symbols = all_symbols - symbols
		for symbol in new_symbols:
			documents.append({"symbol": symbol})
		if documents:
			self.finance_db.insert_many(documents)

	def _make_requests(self, write_to_file=False):
		all_symbols = []
		for exchange in EXCHANGES:
			try:
				response = urlopen(Request(API_URL.format(exchange)))
				tickers = response.read()
				if write_to_file:
					self._write_tickers_to_file(exchange, tickers)
				ticker_list = self._extract_tickers_from_api_response(tickers)
				all_symbols += ticker_list
			except URLError:
				pass
		all_symbols = filter(lambda x: bool(x), self._filter_all_tickers(map(lambda x: x.split(",")[0].replace('"', '').strip(), all_symbols)))
		new_symbols = set(all_symbols) - set(self.all_symbols)
		self.all_symbols = all_symbols
		self.last_check = datetime.now()
		return new_symbols

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
		return self.all_symbols

	def get_commodities(self):
		return Commodities_Symbols

Financial_Symbols = FinancialSymbols()

if __name__ == "__main__":
	FinancialSymbols().get_all()