from os import environ
from contextlib import contextmanager

from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database

from db.models.schedule import *
from db.models import Base
from app.constants import DEV_ENV_VARS

MYSQL_URI = 'mysql://{}:{}@{}/{}'

class ScheduleDBError(Exception):
	pass

class ScheduleDB():

	def __init__(self):
		self.options_task = None
		self.insider_task = None
		self.commodity_task = None
		self.error = None

		self.user = environ.get('SCHEDULE_DB_USER', DEV_ENV_VARS['SCHEDULE_DB_USER'])
		self.pwd = environ.get('SCHEDULE_DB_PWD', DEV_ENV_VARS['SCHEDULE_DB_PWD'])
		self.host = environ.get('SCHEDULE_DB_HOST', DEV_ENV_VARS['SCHEDULE_DB_HOST'])
		self.db = environ.get('SCHEDULE_DB_NAME', DEV_ENV_VARS['SCHEDULE_DB_NAME'])
		self.mysql_uri = MYSQL_URI.format(self.user, self.pwd, self.host, self.db)

		engine = create_engine(self.mysql_uri, pool_size=20, max_overflow=100)
		if not database_exists(engine.url):
			create_database(engine.url)

		try:
			Base.metadata.create_all(engine, checkfirst=True)
		except:
			pass
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
			# error_msg = e.statement
			# for param in e.params:
			# 	error_msg = error_msg.replace('%s', str(param), 1)
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
		if hasattr(trading_date, 'date'):
			trading_date = trading_date.date()
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
		if hasattr(trading_date, 'date'):
			trading_date = trading_date.date()
		with self.session_scope() as session:
			tasks = session.query(OptionTask.symbol).filter_by(trading_date=trading_date, completed=False)
			return map(lambda x: x.symbol, tasks.all())

	def create_insider_task(self, symbol, trading_date):
		self.insider_task = InsiderTask(symbol=symbol, trading_date=trading_date)
		self.add_to_schedule(self.insider_task)
		return self.error

	def get_incomplete_insider_tasks(self, trading_date):
		if hasattr(trading_date, 'date'):
			trading_date = trading_date.date()
		with self.session_scope() as session:
			tasks = session.query(InsiderTask.symbol).filter_by(trading_date=trading_date, completed=False)
			return map(lambda x: x.symbol, tasks.all())

	def complete_insider_task(self, symbol, trading_date):
		if hasattr(trading_date, 'date'):
			trading_date = trading_date.date()
		with self.session_scope() as session:
			task = session.query(InsiderTask).filter_by(symbol=symbol, trading_date=trading_date).first()
			if not task:
				raise ScheduleDBError('InsiderTask does not exist: {} {}'.format(symbol, trading_date))
			task.completed = True
			session.add(task)

	def create_commodities_task(self, symbol, trading_date):
		self.commodity_task = CommodityTask(symbol=symbol, trading_date=trading_date)
		self.add_to_schedule(self.commodity_task)
		return self.error

	def get_incomplete_commodities_tasks(self, trading_date):
		if hasattr(trading_date, 'date'):
			trading_date = trading_date.date()
		with self.session_scope() as session:
			tasks = session.query(CommodityTask.symbol).filter_by(trading_date=trading_date, completed=False)
			return map(lambda x: x.symbol, tasks.all())

	def complete_commodities_task(self, symbol, trading_date):
		if hasattr(trading_date, 'date'):
			trading_date = trading_date.date()
		with self.session_scope() as session:
			task = session.query(CommodityTask).filter_by(symbol=symbol, trading_date=trading_date).first()
			if not task:
				raise ScheduleDBError('CommodityTask does not exist: {} {}'.format(symbol, trading_date))
			task.completed = True
			session.add(task)
