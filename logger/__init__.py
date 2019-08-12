from os import path, mkdir
from datetime import datetime
import logging
from sys import stdout

from app.config import App_Config

BASE_PATH = path.dirname(path.abspath(__file__))

THREAD_LEN = 20

class AppLogger():

	def __init__(self, output_process=False):
		self.logger = None
		self.env = App_Config.env
		self.log_path = path.join(BASE_PATH, 'logs', self.env)
		self._create_log_folders()
		file_name = datetime.now().isoformat().split('.')[0].replace(':', '-') + '.log'

		# doesnt work
		# if output_process is True:
		# 	file_name = "output_" + file_name
		# 	print file_name

		logging.basicConfig(filename=path.join(BASE_PATH, 'logs', self.env, file_name), level=logging.INFO, format='%(asctime)s | %(levelname)7s | %(message)s')
		ch = logging.StreamHandler(stdout)
		self.logger = logging.getLogger(self.env)
		self.logger.addHandler(ch)

	def _create_log_folders(self):
		if not path.exists(self.log_path):
			mkdir(self.log_path)

	# def set_env(self, env):
	# 	self.env = env
	# 	self.log_path = path.join(BASE_PATH, 'logs', self.env)
	# 	self._create_log_folders()
	# 	file_name = datetime.now().isoformat().split('.')[0].replace(':', '-') + '.log'
	# 	logging.basicConfig(filename=path.join(BASE_PATH, 'logs', self.env, file_name), level=logging.INFO, format='%(asctime)s | %(levelname)7s | %(message)s')
	# 	ch = logging.StreamHandler(stdout)
	# 	self.logger = logging.getLogger(self.env)
	# 	self.logger.addHandler(ch)

	def log(self, msg, level='info', threadname=None):
		if threadname:
			msg_format = "%" + str(THREAD_LEN) + "s: "
			msg = (msg_format % threadname) + str(msg)
		if self.logger:
			func = getattr(self.logger, level)
			func(msg)
		else:
			print msg

	def progress(self, iter, task, update_percent=10):
		length = len(iter)
		last_benchmark = 0
		for num, i in enumerate(iter):
			if num/float(length) * 100 >= last_benchmark:
				msg_format = "%" + str(THREAD_LEN) + "s: "
				msg = (msg_format % task) + "{}/{} {}%".format(num, length, "%.2f" % (num/float(length)*100))
				self.log(msg)
				last_benchmark += update_percent
			yield i

Logger = AppLogger()
