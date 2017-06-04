import json
from datetime import datetime
import itertools
import calendar

import requests

VALID_RANGES = ("1d","5d","1mo","3mo","6mo","1y","2y","5y","10y","ytd","max")
VALID_INTERVALS = ("1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo")

BASE_URL = "https://query2.finance.yahoo.com/v7/finance/chart/{}?"
QUERY_URL = "period1={}&period2={}&interval={}"
END_URL = "&indicators=quote&includeTimestamps=true&includePrePost=true&events=div%7Csplit%7Cearn&corsDomain=finance.yahoo.com"


class QuoteError(Exception):
	pass

class Quote(object):

	def __init__(self, symbol, period1=None, period2=None, interval='1d'):
		self.symbol = symbol
		if interval.lower() not in VALID_INTERVALS:
			raise QuoteError('Invalid range parameter: {}'.format(interval))
		self.interval = interval
		if period1 and period2:
			if isinstance(period1, datetime):
				period1 = calendar.timegm(period1.utctimetuple())
			if isinstance(period2, datetime):
				period2 = calendar.timegm(period2.utctimetuple())
		self.period1 = period1
		self.period2 = period2
		if period1 > period2:
			raise QuoteError('Invalid periods, period 2 must be greater than period 1')
		self._make_url()
		self._make_request()
		if not self.response.is_valid_quote():
			raise QuoteError('Invalid symbol: {}'.format(symbol))

	def _make_url(self):
		url = BASE_URL.format(self.symbol)
		if self.period1 and self.period2:
			url += "period1={}&period2={}".format(self.period1, self.period2)
		url += "&interval={}".format(self.interval)
		url += END_URL
		self.url = url

	def _make_request(self):
		req = requests.get(self.url)
		body = {}
		if hasattr(req, 'text'):
			try:
				body = json.loads(req.text)
			except:
				pass
		self.response = QuoteResponse(body)

	def get_data(self):
		return self.response.get_data()


class QuoteResponse():

	def __init__(self, response):
		if not isinstance(response, dict):
			raise QuoteError('Invalid type supplied to QuoteResponse: {}'.format(response.__name__))
		self.response = response
		self.data = None
		self.parse_data()

	def is_valid_quote(self):
		return bool(self.data)

	def parse_data(self):
		data = {}
		if self.response:
			try:
				timestamps = self.response['chart']['result'][0]['timestamp']
				indicators = self.response['chart']['result'][0]['indicators']['quote'][0]
				meta = self.response['chart']['result'][0]['meta']
			except (IndexError, KeyError, TypeError):
				pass
			else:
				data['meta'] = meta
				close = [] if 'close' not in indicators.keys() else indicators['close']
				op = [] if 'open' not in indicators.keys() else indicators['open']
				low = [] if 'low' not in indicators.keys() else indicators['low']
				high = [] if 'high' not in indicators.keys() else indicators['high']
				volume = [] if 'volume' not in indicators.keys() else indicators['volume']

				if all((close, op, low, high, volume, timestamps)):
					dataArr = []
					for ts, cl, o, lo, hi, vol in itertools.izip(timestamps, close, op, low, high, volume):
						dataArr.append((datetime.fromtimestamp(ts), {
						'open': o,
						'high': hi,
						'close': cl,
						'low': lo,
						'volume': vol
						}))
					data['data'] = dataArr
		self.data = data

	def get_data(self):
		return self.data


class Candlestick():

	def __init__(self, close, op, low, high, volume):
		self.close = close
		self.open = op
		self.low = low
		self.high = high
		self.volume = volume

	def get_dict(self):
		return {
			'open': self.open,
			'high': self.high,
			'close': self.close,
			'low': self.low,
			'volume': self.volume
		}