from datetime import datetime, timedelta, date

from app.thread import FThread
from tickers import schedule_tickers, tickers_task_exists
from insider import schedule_insider
from options import schedule_options
from logger import Logger

class Scheduler(FThread):

	def __init__(self):
		super(Scheduler, self).__init__()
		self.thread_name = 'Scheduler'

	def _run(self):
		trading_date = date.today()
		if trading_date.weekday() > 4:
			Logger.log('{}: Not running scheduler on weekend: {}'.format(self.thread_name, trading_date))
		else:
			Logger.log('{}: using {} as trading day'.format(self.thread_name, trading_date))

			Logger.log('{}: running task {}'.format(self.thread_name, schedule_options.func_name))
			transactions = schedule_options(trading_date)
			Logger.log('{}: {} options tasks scheduled'.format(self.thread_name, len(filter(lambda x: x['error'] is False, transactions))))
			Logger.log('{}: {} options tasks errors'.format(self.thread_name, len(filter(lambda x: x['error'] is True, transactions))))
			Logger.log('{}: running task {}'.format(self.thread_name, schedule_insider.func_name))
			transactions = schedule_insider(trading_date)
			Logger.log('{}: {} insider tasks scheduled'.format(self.thread_name, len(filter(lambda x: x['error'] is False, transactions))))
			Logger.log('{}: {} insider tasks errors'.format(self.thread_name, len(filter(lambda x: x['error'] is True, transactions))))

	def _sleep(self):
		# Sleep until next day
		now = datetime.now()
		tomorrow = now + timedelta(days=1)
		tomorrow = datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day, hour=0, minute=0, second=0)
		x = (tomorrow-now).total_seconds()
		self.sleep_time = x
