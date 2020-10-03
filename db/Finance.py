from contextlib import contextmanager
import os
import sys
from pickle import dumps

from pymongo import MongoClient
from pymongo.errors import BulkWriteError, DuplicateKeyError

from app.constants import DEV_ENV_VARS, PROD_ENV_VARS
from utils.credentials import Credentials
from core.StockDbBase import StockDbBase
from utils.batch import batch
from app.config import App_Config

INSERT_BATCH_SIZE = 100

class FinanceDBError(Exception):
	pass

class FinanceDB(StockDbBase):

	__client_dict = {}

	def __init__(self):
		super(FinanceDB, self).__init__()
		App_Config.set_config(sys.argv)
		self.credentials = Credentials()
		self.user = self.credentials.get_user()
		self.password = self.credentials.get_password()

		env_vars = DEV_ENV_VARS if App_Config.env == 'dev' else PROD_ENV_VARS


		self.host = os.environ['FINANCE_DB_HOST']
		self.port = int(os.environ['FINANCE_DB_PORT'])
		self.db_name = os.environ['FINANCE_DB_NAME']

		self.mongo_client = FinanceDB.get_client(self)
		# connection_string = 'mongodb://' + str(self.user) + ':' + str(self.password) + '@' + str(self.host), self.port
		# connection_string = 'mongodb://' + str(self.user) + ':' + str(self.password) + '@' + str(self.host) + ':' + str(self.port)
		# # self.log('mongodb://' + str(self.user) + ':' + '*****' + '@' + str(self.host) + ':' + str(self.port) + "-" + self.db_name)
		# self.mongo_client = MongoClient(connection_string, connect=False)[self.db_name]


	@classmethod
	def get_client(cls, s):
		pid = os.getpid()
		if pid not in cls.__client_dict.keys():
			credentials = Credentials()
			user = credentials.get_user()
			password = credentials.get_password()
			env_vars = DEV_ENV_VARS if App_Config.env == 'dev' else PROD_ENV_VARS

			host = env_vars['FINANCE_DB_HOST']
			port = int(env_vars['FINANCE_DB_PORT'])
			db_name = env_vars['FINANCE_DB_NAME']
			connection_string = 'mongodb://' + str(user) + ':' + str(password) + '@' + str(host) + ':' + str(port)
			cls.log(s, 'New connection: {}'.format(pid))
			cls.__client_dict[pid] = MongoClient(connection_string, connect=False)[db_name]
		return cls.__client_dict[pid]

	def get_db_params(self):
		self.log("using db: {}".format(self.db_name))

	# @contextmanager
	# def mongo_client(self):
	# 	try:
	# 		self.client = MongoClient('mongodb://' + self.user + ':' + self.password + '@' + self.host, self.port, connect=False)
	# 		yield self.client
	# 	except Exception as e:
	# 		print e
	# 	finally:
	# 		print 'Client closed'
	# 		self.client.close()

	def insert_one(self, collection_name, document):
		try:
			collection = self.mongo_client.get_collection(collection_name)
			collection.insert_one(document)
		except DuplicateKeyError:
			pass


	def insert(self, collection_name, documents, bypass_document_validation=False):
		try:
			if isinstance(documents, dict):
				documents = (documents,)
			collection = self.mongo_client.get_collection(collection_name)
			for document_batch in batch(documents, INSERT_BATCH_SIZE):
				collection.insert_many(document_batch, bypass_document_validation=bypass_document_validation)
		except BulkWriteError as e:
			self.log('Documents: {}'.format(documents))
			self.log_exception(e)
			raise e
		except MemoryError as e:
			self.log('Documents size: {}'.format(sys.getsizeof(dumps(documents))))
			self.log_exception(e)
			raise e

	def aggregate(self, collection_name):
		collection = self.mongo_client.get_collection(collection_name)
		return collection.aggregate()

	def find(self, collection_name, query, fields):
		fields['_id'] = False
		try:
			collection = self.mongo_client.get_collection(collection_name)
			return collection.find(query, fields)
		except MemoryError as e:
			self.log_exception(e)
			self.log('collection: {}'.format(collection_name))
			self.log('query: {}'.format(query))
			raise e

	def replace_one(self, collection_name, filter, document, upsert=False):
		collection = self.mongo_client.get_collection(collection_name)
		collection.replace_one(filter, document, upsert=upsert)

	def create_index(self, collection_name, index_name, keys, unique=False, expireAfterSeconds=None):
		collection = self.mongo_client.get_collection(collection_name)
		if expireAfterSeconds is not None:
			collection.create_index(keys, name=index_name, unique=unique, expireAfterSeconds=expireAfterSeconds)
		else:
			collection.create_index(keys, name=index_name, unique=unique)

	def drop_indexes(self, collection_name):
		collection = self.mongo_client.get_collection(collection_name)
		collection.drop_indexes()

	def distinct(self, collection_name, field, query):
		collection = self.mongo_client.get_collection(collection_name)
		return collection.distinct(field, query)

	def delete_many(self, collection_name, query):
		collection = self.mongo_client.get_collection(collection_name)
		collection.delete_many(query)
