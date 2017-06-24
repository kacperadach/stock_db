from os import environ
from datetime import datetime
import time

from constants import DEV_ENV_VARS, PROD_ENV_VARS
from schedule import Scheduler
from acquisition.Acquirer import Acquirer
from acquisition.historical import HistoricalAcquisition
from logger import Logger

class MainService():

	def __init__(self, official=False):
		self.official = official
		self.env = 'prod' if self.official else 'dev'
		Logger.set_env(self.env)
		Logger.log('Service: Running application in {} environment'.format(self.env))
		self.initialize_env_vars()
		Logger.log('+---------------------------------------------+')
		Logger.log('|                                             |')
		Logger.log('|    Service Started at {}      |'.format(datetime.now().strftime('%H:%M %Y-%m-%d')))
		Logger.log('|          Environment: {}                  |'.format(self.env.upper() if self.env == 'prod' else 'DEV '))
		Logger.log('|                                             |')
		Logger.log('+---------------------------------------------+')
		self.main_service()

	def main_service(self):
		self.Scheduler = Scheduler()
		self.Acquirer = Acquirer()
		self.HistoricalAcquisition = HistoricalAcquisition(AcquirerThread=self.Acquirer)
		self.Scheduler.start()
		self.Acquirer.start()
		self.HistoricalAcquisition.start()

	def initialize_env_vars(self):
		environment_vars = PROD_ENV_VARS if self.official else DEV_ENV_VARS
		environ['env'] = self.env
		for key, val in environment_vars.items():
			environ[key] = val
