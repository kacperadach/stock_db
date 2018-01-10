from datetime import datetime
import itertools
import calendar
from pytz import timezone

VALID_RANGES = ("1d","5d","1mo","3mo","6mo","1y","2y","5y","10y","ytd","max")
VALID_INTERVALS = ("1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo")
BASE_URL = "https://query2.finance.yahoo.com/v7/finance/chart/{}?"
QUERY_URL = "period1={}&period2={}&interval={}"
END_URL = "&indicators=quote&includeTimestamps=true&includePrePost=true&events=div%7Csplit%7Cearn&corsDomain=finance.yahoo.com"
ACCEPTED_TIMEZONES = ('EST', 'UTC')

class YahooFinanceStockRequestError(Exception):
	pass

class YahooFinanceStockRequest():
	"""
	Perdiod1 and Period2 should be datetime objects with EST or UTC timezone
	"""
	def __init__(self, symbol, period1, period2, interval='1m'):
		self._validate_inputs(period1, period2, interval)
		if period1.tzinfo == timezone('EST'):
			period1 = period1.astimezone(timezone('UTC'))
			period2 = period2.astimezone(timezone('UTC'))
		period1 = calendar.timegm(period1.utctimetuple())
		period2 = calendar.timegm(period2.utctimetuple())
		self.symbol = symbol
		self.interval = interval
		self.period1 = period1
		self.period2 = period2
		self._make_url()

	def _validate_inputs(self, period1, period2, interval):
		if not period1 or not period2:
			raise YahooFinanceStockRequestError('Period1 or Period2 were not set')
		if not isinstance(period1, datetime) or not isinstance(period2, datetime):
			raise YahooFinanceStockRequestError('Period1 or Period2 are not datetime')
		if not period1.tzinfo or not period2.tzinfo or period1.tzinfo.zone not in ACCEPTED_TIMEZONES or period2.tzinfo.zone not in ACCEPTED_TIMEZONES:
			raise YahooFinanceStockRequestError('Period1 or Period2 do not have appropriate timezone')
		if period1.tzinfo != period2.tzinfo:
			raise YahooFinanceStockRequestError('Period1 or Period2 do not have the same timezone')
		if period1 > period2:
			raise YahooFinanceStockRequestError('Period2 must be greater than Period1')
		if interval.lower() not in VALID_INTERVALS:
			raise YahooFinanceStockRequestError('Invalid interval parameter: {}'.format(interval))

	def get_url(self):
		return self.url

	@staticmethod
	def parse_response(response):
		data = {'data': [], 'meta': {}, 'events': []}
		indicators = None
		timestamps = None
		unadjclose = None
		adjclose = None
		if response:
			if isinstance(response, dict) and 'chart' in response.keys():
				if isinstance(response['chart'], dict) and 'result' in response['chart'].keys():
					if isinstance(response['chart']['result'], list) and len(response['chart']['result']) > 0:
						result = response['chart']['result'][0]
						if isinstance(result, dict):
							if 'meta' in result.keys():
								data['meta'] = result['meta']
							if 'timestamp' in result.keys():
								timestamps = result['timestamp']
							if 'events' in result.keys():
								data['events'] = result['events']
							if 'indicators' in result.keys() and isinstance(result['indicators'], dict):
								if 'quote' in result['indicators'].keys() and isinstance(result['indicators']['quote'], list) and len(result['indicators']['quote']) > 0:
									indicators = result['indicators']['quote'][0]
								if 'unadjclose' in result['indicators'].keys() and isinstance(result['indicators']['unadjclose'], list) and len(result['indicators']['unadjclose']) > 0 and isinstance(result['indicators']['unadjclose'][0], dict) and 'unadjclose' in result['indicators']['unadjclose'][0].keys():
									unadjclose = result['indicators']['unadjclose'][0]['unadjclose']
								if 'adjclose' in result['indicators'].keys() and isinstance(result['indicators']['adjclose'], list) and len(result['indicators']['adjclose']) > 0 and isinstance(result['indicators']['adjclose'][0], dict) and 'adjclose' in result['indicators']['adjclose'][0].keys():
									adjclose = result['indicators']['adjclose'][0]['adjclose']

		if indicators and timestamps:
			close = [] if 'close' not in indicators.keys() else indicators['close']
			op = [] if 'open' not in indicators.keys() else indicators['open']
			low = [] if 'low' not in indicators.keys() else indicators['low']
			high = [] if 'high' not in indicators.keys() else indicators['high']
			volume = [] if 'volume' not in indicators.keys() else indicators['volume']

			if all((close, op, low, high, volume, timestamps)):
				data_arr = []
				if unadjclose and adjclose:
					for ts, cl, o, lo, hi, vol, adj, unadj in itertools.izip(timestamps, close, op, low, high, volume, adjclose, unadjclose):
						data_arr.append((datetime.fromtimestamp(ts), {
							'open': o,
							'high': hi,
							'close': cl,
							'low': lo,
							'volume': vol,
							'adjclose': adj,
							'unadjclose': unadj
						}))
					data['data'] = data_arr
				else:
					for ts, cl, o, lo, hi, vol in itertools.izip(timestamps, close, op, low, high, volume):
						data_arr.append((datetime.fromtimestamp(ts), {
							'open': o,
							'high': hi,
							'close': cl,
							'low': lo,
							'volume': vol
						}))
					data['data'] = data_arr
		return data

	def _make_url(self):
		url = BASE_URL.format(self.symbol)
		if self.period1 and self.period2:
			url += "period1={}&period2={}".format(self.period1, self.period2)
		url += "&interval={}".format(self.interval)
		url += END_URL
		self.url = url
