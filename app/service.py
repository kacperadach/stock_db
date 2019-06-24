from os import environ
from datetime import datetime
import sys

from constants import DEV_ENV_VARS, PROD_ENV_VARS

from logger import Logger
from discord.webhook import DiscordWebhook
from db.MongoIndexer import MongoIndexer
from db.Finance import Finance_DB
from config import App_Config
from core.ScraperQueueManager import ScraperQueueManager
from request.base.TorManager import Tor_Manager

class MainService():

	def __init__(self):
		self.start = datetime.now()
		self.thread_name = 'MainService'
		App_Config.set_config(sys.argv)
		self.env = App_Config.env
		self.use_tor = App_Config.use_tor
		Logger.set_env(self.env)
		self._log(sys.argv)
		self._log('Running application in {} environment'.format(self.env))
		self.initialize_env_vars()
		Finance_DB.get_db_params()
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
		Logger.log('Service finished, total time running: {}'.format(datetime.now()-self.start))

	def _log(self, msg, level='info'):
		Logger.log(msg, level=level, threadname=self.thread_name)

	def main_service(self):
		if self.use_tor:
			self._log('Starting Tor instances')
			Tor_Manager.start_instances()
			self._log('Running Tor Test')
			Tor_Manager.test()
		MongoIndexer().create_indices()
		ScraperQueueManager(use_tor=self.use_tor).start()

	def initialize_env_vars(self):
		environment_vars = PROD_ENV_VARS if self.env.lower() == 'prod' else DEV_ENV_VARS
		environ['env'] = self.env
		for key, val in environment_vars.items():
			Logger.log(str(key) + " " + str(val))
			environ[key] = val
