from os import environ
from contextlib import contextmanager

from pymongo import MongoClient
from pymongo.errors import BulkWriteError

from app.constants import DEV_ENV_VARS
from utils.credentials import Credentials
from logger import Logger
from core.StockDbBase import StockDbBase

QUEUE_TIMEOUT = 30
OUTPUT_QUEUE_SIZE = 100

class FinanceDBError(Exception):
	pass

class FinanceDB(StockDbBase):

	def __init__(self):
		super(FinanceDB, self).__init__()
		self.credentials = Credentials()
		self.user = self.credentials.get_user()
		self.password = self.credentials.get_password()
		self.host = environ.get('FINANCE_DB_HOST', DEV_ENV_VARS['FINANCE_DB_HOST'])
		self.port = int(environ.get('FINANCE_DB_PORT', DEV_ENV_VARS['FINANCE_DB_PORT']))
		self.db_name = environ.get('FINANCE_DB_NAME', DEV_ENV_VARS['FINANCE_DB_NAME'])
		self.mongo_client = MongoClient('mongodb://' + self.user + ':' + self.password + '@' + self.host, self.port)[self.db_name]

	def get_db_params(self):
		self.host = environ.get('FINANCE_DB_HOST', DEV_ENV_VARS['FINANCE_DB_HOST'])
		self.port = int(environ.get('FINANCE_DB_PORT', DEV_ENV_VARS['FINANCE_DB_PORT']))
		self.db_name = environ.get('FINANCE_DB_NAME', DEV_ENV_VARS['FINANCE_DB_NAME'])
		self.log("using db: {}".format(self.db_name))

	@contextmanager
	def mongo_client(self):
		try:
			self.client =  MongoClient('mongodb://' + self.user + ':' + self.password + '@' + self.host, self.port)
			yield self.client
		except Exception as e:
			print e
		finally:
			self.client.close()

	def insert(self, collection_name, documents):
		try:
			if isinstance(documents, dict):
				documents = (documents,)
			collection = self.mongo_client.get_collection(collection_name)
			collection.insert_many(documents)
		except BulkWriteError as e:
			self.log_exception(e)
			raise e

	def find(self, collection_name, query, fields):
		collection = self.mongo_client.get_collection(collection_name)
		return collection.find(query, fields)

	def replace_one(self, collection_name, filter, document, upsert=True):
		collection = self.mongo_client.get_collection(collection_name)
		collection.replace_one(filter, document, upsert=upsert)

	def create_index(self, collection_name, index_name, keys, unique=False):
		collection = self.mongo_client.get_collection(collection_name)
		collection.create_index(keys, name=index_name, unique=unique)

Finance_DB = FinanceDB()
