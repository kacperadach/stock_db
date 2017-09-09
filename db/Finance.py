from os import environ
from contextlib import contextmanager
from Queue import Queue, Empty
from threading import Thread

from pymongo import MongoClient

from app.constants import DEV_ENV_VARS
from utils.credentials import Credentials
from ReportDecorator import report
from logger import Logger

QUEUE_TIMEOUT = 30
OUTPUT_QUEUE_SIZE = 100

class FinanceDBError(Exception):
	pass

class FinanceDB():

	def __init__(self, collection=None):
		self.name = "FinanceDB"
		self.credentials = Credentials()
		self.user = self.credentials.get_user()
		self.password = self.credentials.get_password()
		self.host =  environ.get('FINANCE_DB_HOST', DEV_ENV_VARS['FINANCE_DB_HOST'])
		self.port = int(environ.get('FINANCE_DB_PORT', DEV_ENV_VARS['FINANCE_DB_PORT']))
		self.db_name = environ.get('FINANCE_DB_NAME', DEV_ENV_VARS['FINANCE_DB_NAME'])
		self.collection = collection
		self.queue = Queue(maxsize=1)
		self.output_queue = Queue(maxsize=OUTPUT_QUEUE_SIZE)
		self.queue_worker = Thread(target=self.queue_worker)
		self.queue_worker.setDaemon(True)
		self.queue_running = False

	def _log(self, msg, level='info'):
		Logger.log(msg, level=level, threadname=self.name)

	def _start_queue_worker(self):
		if self.queue_running:
			return
		self.queue_running = True
		self.queue_worker.start()

	def queue_worker(self):
		self._log('Starting Queue Worker')
		try:
			with self.mongo_client() as db:
				self.db = db
				while 1:
					queue_item = self.queue.get(timeout=QUEUE_TIMEOUT)
					data = queue_item['method'](queue_item)
					if data is not None:
						self.output_queue.put(data)
		except Empty:
			self.queue_running = False
			self._log('Empty Queue shutting down')

	@contextmanager
	def mongo_client(self):
		try:
			self._check_collection()
			self.client =  MongoClient('mongodb://' + self.user + ':' + self.password + '@' + self.host, self.port)
			yield self.client[self.db_name]
		except Exception as e:
			print e
		finally:
			self.client.close()

	def set_collection(self, collection_name):
		self.collection = collection_name

	def _check_collection(self):
		if not self.collection:
			raise FinanceDBError('Collection not set, use FinanceDB.set_collection')

	@report
	def insert_one(self, document):
		self._start_queue_worker()
		self.queue.put({"collection": self.collection, "documents": document, "method": self._insert})

	@report
	def insert_many(self, documents):
		self._start_queue_worker()
		self.queue.put({"collection": self.collection, "documents": documents, "method": self._insert})

	def _insert_reporting(self, document):
		self.collection = 'reporting'
		self._start_queue_worker()
		self.queue.put({"collection": self.collection, "documents": document, "method": self._insert})

	def _insert(self, queue_item):
		documents = queue_item['documents']
		collection = self.db.get_collection(self.collection)
		if isinstance(documents, dict):
			documents = (documents,)
		collection.insert_many(documents)

	def find(self, query, fields=None):
		self._start_queue_worker()
		self.queue.put({"collection": self.collection, "query": query, "fields": fields, "method": self._find})
		return self.output_queue.get()

	def _find(self, queue_item):
		collection = self.db.get_collection(queue_item["collection"])
		return collection.find(queue_item["query"], queue_item["fields"])

	def save(self, document):
		self._start_queue_worker()
		self.queue.put({"collection": self.collection, "document": document,  "method": self._save})

	def _save(self, queue_item):
		collection = self.db.get_collection(self.collection)
		collection.save(queue_item["document"])

	def create_index(self, keys):
		self._start_queue_worker()
		self.queue.put({"collection": self.collection, "keys": keys, "method": self._create_index})

	def _create_index(self, queue_item):
		collection = self.db.get_collection(self.collection)
		collection.create_index(queue_item["keys"])

Finance_DB = FinanceDB()