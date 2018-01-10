
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
