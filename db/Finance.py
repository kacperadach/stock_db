from os import environ

from pymongo import MongoClient

class FinanceDBError(Exception):
	pass

class FinanceDB():

	def __init__(self):
		self.host =  environ['FINANCE_DB_HOST']
		self.port = int(environ['FINANCE_DB_PORT'])
		self.db_name = environ['FINANCE_DB_NAME']
		self.collection = None
		self.client = MongoClient(self.host, self.port)
		self.db = self.client[self.db_name]

	def set_collection(self, collection_name):
		self.collection = collection_name

	def _check_collection(self):
		if not self.collection:
			raise FinanceDBError('Collection not set, use FinanceDB.set_collection')

	def insert_one(self, document):
		self._check_collection()
		collection = self.db.get_collection(self.collection)
		collection.insert_one(document)

	def insert_many(self, documents):
		self._check_collection()
		collection = self.db.get_collection(self.collection)
		collection.insert_many(documents)

	def find_one(self, query):
		self._check_collection()
		return self.collection.find_one(query)

	def find(self, query):
		self._check_collection()
		return self.collection.find(query)
