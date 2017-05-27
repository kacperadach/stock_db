from os import environ
from contextlib import contextmanager

from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database

from db.models.schedule import *
from db.models import Base
from logger import Logger

MYSQL_URI = 'mysql://{}:{}@{}/{}'

class ScheduleDBError(Exception):
	pass

class ScheduleDB():

	def __init__(self):
		self.options_task = None
		self.error = None

		self.user = environ['SCHEDULE_DB_USER']
		self.pwd = environ['SCHEDULE_DB_PWD']
		self.host = environ['SCHEDULE_DB_HOST']
		self.db = environ['SCHEDULE_DB_NAME']
		self.mysql_uri = MYSQL_URI.format(self.user, self.pwd, self.host, self.db)

		engine = create_engine(self.mysql_uri)
		if not database_exists(engine.url):
			create_database(engine.url)

		Base.metadata.create_all(engine, checkfirst=True)
		self.Session = sessionmaker(bind=engine)

	@contextmanager
	def session_scope(self):
		self.error = False
		session = self.Session()
		try:
			yield session
			session.commit()
		except Exception as e:
			self.error = True
			session.rollback()
			error_msg = e.statement
			for param in e.params:
				error_msg = error_msg.replace('%s', str(param), 1)
			#Logger.log('Schedule Database Rollback: {}'.format(error_msg), 'warning')
		finally:
			session.close()

	def add_to_schedule(self, obj):
		with self.session_scope() as session:
			if isinstance(obj, (list, tuple)):
				session.add_all(obj)
			else:
				session.add(obj)

	def query(self, table, filter_dict):
		with self.session_scope() as session:
			return session.query(table).filter_by(**filter_dict)

	def create_options_task(self, symbol, trading_date):
		self.options_task = OptionTask(symbol=symbol, trading_date=trading_date)
		self.add_to_schedule(self.options_task)
		return self.error

	def complete_options_task(self, symbol, trading_date):
		with self.session_scope() as session:
			task = session.query(OptionTask).filter_by(symbol=symbol, trading_date=trading_date).first()
			if not task:
				raise ScheduleDBError('OptionsTask does not exist: {} {}'.format(symbol, trading_date))
			task.completed = True
			session.add(task)

	def create_tickers_task(self, trading_date):
		self.add_to_schedule(TickerTask(trading_date=trading_date))

	def complete_tickers_task(self, trading_date):
		with self.session_scope() as session:
			task = session.query(TickerTask).filter_by(trading_date=trading_date).first()
			if not task:
				raise ScheduleDB('TickerTask does not exist for date: {}'.format(trading_date))
			task.completed = True
			session.add(task)

	def get_incomplete_options_tasks(self, trading_date):
		with self.session_scope() as session:
			tasks = session.query(OptionTask.symbol).filter_by(trading_date=trading_date, completed=False)
			for task in tasks:
				yield task.symbol
