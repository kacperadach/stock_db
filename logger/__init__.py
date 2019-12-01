from os import path, mkdir, remove
import logging
import logging.handlers
from logging.handlers import RotatingFileHandler
from sys import stdout

from app.config import App_Config

BASE_PATH = path.dirname(path.abspath(__file__))

THREAD_LEN = 20

class AppLogger():

	def __init__(self, file_name='scraper.log'):
		self.env = App_Config.env
		self.log_path = path.join(BASE_PATH, 'logs', self.env)
		self.file_name = file_name

		self.logger = logging.getLogger(file_name)
		if len(self.logger.handlers) == 0:
			self._create_log_folders()
			log_file_path = path.join(BASE_PATH, 'logs', self.env, self.file_name)

			# logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s | %(levelname)7s | %(message)s')

			# self.logger = logging.getLogger(self.env)
			self.logger.setLevel(logging.DEBUG)
			# logging.handlers.RotatingFileHandler()
			handler = RotatingFileHandler(log_file_path, maxBytes=1024000 * 5, backupCount=2)
			# handler = RotatingFileHandler(log_file_path, maxBytes=1024 * 10, backupCount=2)

			ch = logging.StreamHandler(stdout)
			handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)7s | %(message)s'))
			self.logger.addHandler(handler)
			self.logger.addHandler(ch)


	def _create_log_folders(self):
		if not path.exists(self.log_path):
			mkdir(self.log_path)

	def get_file_name(self):
		return self.file_name

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
			print(msg)

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
