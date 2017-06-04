from datetime import datetime, timedelta, date

from app.thread import FThread
from insider import schedule_insider
from options import schedule_options
from commodities import schedule_commodities
from logger import Logger

class Scheduler(FThread):

	def __init__(self):
		super(Scheduler, self).__init__()
		self.thread_name = 'Scheduler'

	def _run(self):
		self.trading_date = date.today()
		if self.trading_date.weekday() > 4:
			Logger.log('{}: Not running scheduler on weekend: {}'.format(self.thread_name, self.trading_date))
		else:
			Logger.log('{}: using {} as trading day'.format(self.thread_name, self.trading_date))
			self.schedule(schedule_options, 'options')
			self.schedule(schedule_insider, 'insider')
		self.schedule(schedule_commodities, 'commodities')

	def schedule(self, task, name):
		Logger.log('{}: running task {}'.format(self.thread_name, task.func_name))
		transactions = task(self.trading_date)
		Logger.log('{}: {} {} tasks scheduled'.format(self.thread_name, len(filter(lambda x: x['error'] is False, transactions)), name))
		Logger.log('{}: {} {} tasks errors'.format(self.thread_name, len(filter(lambda x: x['error'] is True, transactions)), name))

	def _sleep(self):
		# Sleep until next day
		now = datetime.now()
		tomorrow = now + timedelta(days=1)
		tomorrow = datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day, hour=0, minute=0, second=0)
		x = (tomorrow-now).total_seconds()
		self.sleep_time = x
