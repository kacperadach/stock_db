import time
from threading import Thread
from logger import Logger

class FThread(Thread):

	def __init__(self):
		super(FThread, self).__init__()

	def run(self):
		try:
			while 1:
				self._run()
				self._sleep()
		except KeyboardInterrupt:
			Logger.log('Keyboard Interrupt, closing thread')


	def _run(self):
		pass

	def sleep(self):
		pass

