from datetime import datetime, timedelta, date

from app.thread import FThread
from insider import schedule_insider
from options import schedule_options
from commodities import schedule_commodities
from logger import Logger

LOG_CHAR_COUNT = 9

class Scheduler(FThread):

	def __init__(self):
		super(Scheduler, self).__init__()
		self.thread_name = 'Scheduler'

	def _run(self):
		self.trading_date = date.today()
		if self.trading_date.weekday() > 4:
			self._log('Not running scheduler on weekend: {}'.format(self.trading_date))
		else:
			self._log('using {} as trading day'.format(self.trading_date))
			self.schedule(schedule_options, 'options')
			self.schedule(schedule_insider, 'insider')
		self.schedule(schedule_commodities, 'commodities')

	def schedule(self, task, name):
		self._log('running task {}'.format(task.func_name))
		transactions = task(self.trading_date)
		self._log('{} {} tasks scheduled'.format(len(filter(lambda x: x['error'] is False, transactions)), name))
		self._log('{} {} tasks errors'.format(len(filter(lambda x: x['error'] is True, transactions)), name))

	def _sleep(self):
		# Sleep until next day
		now = datetime.now()
		tomorrow = now + timedelta(days=1)
		tomorrow = datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day, hour=0, minute=0, second=0)
		x = (tomorrow-now).total_seconds()
		self.sleep_time = x
