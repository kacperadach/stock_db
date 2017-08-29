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

COMMAND_LINE_ARGUMENTS = {
	'env': 'dev',
	'use_tor': False
}


class MainService():

	def __init__(self):
		self._parse_cl_arguments(sys.argv)
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
		if self.env == 'prod':
			DiscordWebhook().alert_start()

	def main_service(self):
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

	def _parse_cl_arguments(self, argv):
		if len(argv) <= 1:
			pass
		else:
			set_keys = []
			for i in range(1, len(argv)):
				key, value = argv[i].split('=')
				if key.lower() in COMMAND_LINE_ARGUMENTS.iterkeys():
					setattr(self, key, value)
					set_keys.append(key)
		for key, value in COMMAND_LINE_ARGUMENTS.iteritems():
			if not hasattr(self, key):
				if isinstance(value, bool):
					setattr(self, key, bool(value))
				else:
					setattr(self, key, value)
