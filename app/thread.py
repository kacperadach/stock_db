import time
import datetime
import traceback
from threading import Thread
from logger import Logger

class FThread(Thread):

	def __init__(self):
		super(FThread, self).__init__()
		self.thread_name = None
		self.sleep_time = None

	def _log(self, msg, level='info'):
		Logger.log(msg, level=level, threadname=self.thread_name)

	def run(self):
		try:
			while 1:
				self._log('Running ' + self.thread_name)
				self._run()
				self._sleep()
				self._log('Sleeping for ' + str(datetime.timedelta(seconds=self.sleep_time)))
				time.sleep(self.sleep_time)
		except Exception as e:
			self._log('unexpected error occured: {}'.format(e))
			Logger.log(traceback.format_exc())

	def _run(self):
		pass

	def _sleep(self):
		pass
