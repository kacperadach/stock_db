from os import environ
from contextlib import contextmanager

from pymongo import MongoClient

from app.constants import DEV_ENV_VARS

class FinanceDBError(Exception):
	pass

class FinanceDB():

	def __init__(self, collection=None):
		self.user = 'admin'
		self.password = 'Thickblunt420'
		self.host =  environ.get('FINANCE_DB_HOST', DEV_ENV_VARS['FINANCE_DB_HOST'])
		self.port = int(environ.get('FINANCE_DB_PORT', DEV_ENV_VARS['FINANCE_DB_PORT']))
		self.db_name = environ.get('FINANCE_DB_NAME', DEV_ENV_VARS['FINANCE_DB_NAME'])
		self.collection = collection

	@contextmanager
	def mongo_client(self):
		try:
			self._check_collection()
			self.client = MongoClient('mongodb://' + self.user + ':' + self.password + '@' + self.host, self.port)
			self.db = self.client[self.db_name]
			yield self.db
		except Exception as e:
			print e
		finally:
			self.client.close()

	def set_collection(self, collection_name):
		self.collection = collection_name

	def _check_collection(self):
		if not self.collection:
			raise FinanceDBError('Collection not set, use FinanceDB.set_collection')

	def insert_one(self, document):
		with self.mongo_client() as db:
			collection = db.get_collection(self.collection)
			collection.insert_one(document)

	def insert_many(self, documents):
		with self.mongo_client() as db:
			collection = db.get_collection(self.collection)
			collection.insert_many(documents)

	def find(self, query):
		with self.mongo_client() as db:
			collection = db.get_collection(self.collection)
			return collection.find(query)
