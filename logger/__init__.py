from os import environ, path, mkdir
from datetime import datetime
import logging
from sys import stdout

BASE_PATH = path.dirname(path.abspath(__file__))

class AppLogger():

	def __init__(self):
		pass

	def _create_log_folders(self):
		if not path.exists(self.log_path):
			mkdir(self.log_path)

	def set_env(self, env):
		self.env = env
		self.log_path = path.join(BASE_PATH, 'logs', self.env)
		self._create_log_folders()
		file_name = datetime.now().isoformat().split('.')[0].replace(':', '-') + '.log'
		logging.basicConfig(filename=path.join(BASE_PATH, 'logs', self.env, file_name), level=logging.INFO)
		ch = logging.StreamHandler(stdout)
		self.logger = logging.getLogger(self.env)
		self.logger.addHandler(ch)

	def log(self, msg, level='info'):
		func = getattr(self.logger, level)
		func(msg)

Logger = AppLogger()