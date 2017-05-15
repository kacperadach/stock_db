from os import environ
from datetime import datetime

from constants import DEV_ENV_VARS, PROD_ENV_VARS
from schedule import Scheduler

class MainService():

	def __init__(self, official=False):
		self.official = official
		self.initialize_env_vars()
		self.main_service()

	def main_service(self):
		s = Scheduler().start()

	def initialize_env_vars(self):
		environment_vars = PROD_ENV_VARS if self.official else DEV_ENV_VARS
		for key, val in environment_vars.items():
			environ[key] = val
