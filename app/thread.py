import time
import datetime
from threading import Thread
from logger import Logger

class FThread(Thread):

	def __init__(self):
		super(FThread, self).__init__()
		self.thread_name = None
		self.sleep_time = None

	def run(self):
		try:
			while 1:
				Logger.log(self.thread_name + ': Running ' + self.thread_name)
				self._run()
				self._sleep()
				Logger.log(self.thread_name + ': Sleeping for ' + str(datetime.timedelta(seconds=self.sleep_time)))
				time.sleep(self.sleep_time)
		except KeyboardInterrupt:
			Logger.log('Keyboard Interrupt, closing thread')

	def _run(self):
		pass

	def _sleep(self):
		pass
