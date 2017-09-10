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

	def __init__(self):
		self.name = "FinanceDB"
		self.credentials = Credentials()
		self.user = self.credentials.get_user()
		self.password = self.credentials.get_password()
		self.host =  environ.get('FINANCE_DB_HOST', DEV_ENV_VARS['FINANCE_DB_HOST'])
		self.port = int(environ.get('FINANCE_DB_PORT', DEV_ENV_VARS['FINANCE_DB_PORT']))
		self.db_name = environ.get('FINANCE_DB_NAME', DEV_ENV_VARS['FINANCE_DB_NAME'])
		self.queue = Queue(maxsize=1)
		self.output_queue = Queue(maxsize=OUTPUT_QUEUE_SIZE)
		self.queue_worker = Thread(target=self.worker)
		self.queue_worker.setDaemon(True)
		self.queue_running = False

	def _log(self, msg, level='info'):
		Logger.log(msg, level=level, threadname=self.name)

	def _start_queue_worker(self):
		if self.queue_running:
			return
		self.queue_running = True
		self.queue_worker.start()

	def worker(self):
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
			self.client =  MongoClient('mongodb://' + self.user + ':' + self.password + '@' + self.host, self.port)
			yield self.client[self.db_name]
		except Exception as e:
			print e
		finally:
			self.client.close()

	@report
	def insert_one(self, document, collection):
		self._start_queue_worker()
		self.queue.put({"collection": collection, "documents": document, "method": self._insert})

	@report
	def insert_many(self, documents, collection):
		self._start_queue_worker()
		self.queue.put({"collection": collection, "documents": documents, "method": self._insert})

	def _insert_reporting(self, document, collection):
		self.collection = 'reporting'
		self._start_queue_worker()
		self.queue.put({"collection": collection, "documents": document, "method": self._insert})

	def _insert(self, queue_item):
		documents = queue_item['documents']
		collection = self.db.get_collection(queue_item['collection'])
		if isinstance(documents, dict):
			documents = (documents,)
		collection.insert_many(documents)

	def find(self, query, collection, fields=None):
		self._start_queue_worker()
		self.queue.put({"collection": collection, "query": query, "fields": fields, "method": self._find})
		return self.output_queue.get()

	def _find(self, queue_item):
		collection = self.db.get_collection(queue_item["collection"])
		return collection.find(queue_item["query"], queue_item["fields"])

	def save(self, document, collection):
		self._start_queue_worker()
		self.queue.put({"collection": collection, "document": document,  "method": self._save})

	def _save(self, queue_item):
		collection = self.db.get_collection(queue_item['collection'])
		collection.save(queue_item["document"])

	def create_index(self, keys, collection):
		self._start_queue_worker()
		self.queue.put({"collection": collection, "keys": keys, "method": self._create_index})

	def _create_index(self, queue_item):
		collection = self.db.get_collection(queue_item['collection'])
		collection.create_index(queue_item["keys"])

Finance_DB = FinanceDB()