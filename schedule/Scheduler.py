import time
from datetime import datetime, timedelta, date

from app.thread import FThread
from tickers import schedule_tickers

class Scheduler(FThread):

	def _run(self):
		trading_date = date.today()
		schedule_tickers(trading_date)

	def _sleep(self):
		# Sleep until next day
		now = datetime.now()
		tomorrow = now + timedelta(days=1)
		tomorrow = datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day, hour=0, minute=0, second=0)
		x = (tomorrow-now).total_seconds()
		time.sleep(x)

		
