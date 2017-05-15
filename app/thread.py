import time
from threading import Thread

class FThread(Thread):

	def __init__(self):
		super(FThread, self).__init__()

	def run(self):
		while 1:
			self._run()
			self._sleep()

	def _run(self):
		pass

	def sleep(self):
		pass

