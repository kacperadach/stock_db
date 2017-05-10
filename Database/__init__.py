from pymongo import MongoClient

class FinanceDB():

	def __init__(self, host='localhost', port=27017):
		self.host = host
		self.port = port
		self.client = MongoClient(host, post)

	def get_collection(self, name='test_collection'):
		return self.client[name]

	def insert_document(self, collection):




