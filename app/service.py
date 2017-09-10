from os import environ
from datetime import datetime
import sys

from constants import DEV_ENV_VARS, PROD_ENV_VARS
from acquisition.Acquirer import Acquirer
from acquisition.historical import HistoricalAcquisition
from reporting.reporting import Reporting
from logger import Logger
from discord.webhook import DiscordWebhook
from db.Indexer import MongoIndexer
from config import App_Config
from request.base.tor_client import TorTest

class MainService():

	def __init__(self):
		self.thread_name = 'MainService'
		App_Config.set_config(sys.argv)
		self._log(sys.argv)
		self.env = App_Config.env
		self.use_tor = App_Config.use_tor
		Logger.set_env(self.env)
		self._log('Running application in {} environment'.format(self.env))
		self.initialize_env_vars()
		Logger.log('+---------------------------------------------+')
		Logger.log('                                             ')
		Logger.log('    Service Started at {}      '.format(datetime.now().strftime('%H:%M %Y-%m-%d')))
		Logger.log('          Environment: {}                   '.format(self.env))
		Logger.log('          use_tor: {}                      '.format(self.use_tor))
		Logger.log('                                             ')
		Logger.log('+---------------------------------------------+')
		self.main_service()
		if self.env == 'prod':
			DiscordWebhook().alert_start()

	def _log(self, msg, level='info'):
		Logger.log(msg, level=level, threadname=self.thread_name)

	def main_service(self):
		if self.use_tor:
			self._log('Running Tor Test')
			TorTest()
		MongoIndexer().create_indices()
		self.Acquirer = Acquirer()
		self.HistoricalAcquisition = HistoricalAcquisition(AcquirerThread=self.Acquirer)
		self.Reporting = Reporting()
		self.Acquirer.start()
		self.HistoricalAcquisition.start()
		self.Reporting.start()

	def initialize_env_vars(self):
		environment_vars = PROD_ENV_VARS if self.env.lower() == 'prod' else DEV_ENV_VARS
		environ['env'] = self.env
		for key, val in environment_vars.items():
			environ[key] = val
