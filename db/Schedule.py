from contextlib import contextmanager

from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database

from db.models import *
from db.models.base import Base

MYSQL_URI = 'mysql://{}:{}@{}/{}'

class ScheduleDBError(Exception):
	pass

class ScheduleDB():

	def __init__(self, user, pwd, host, db):
		self.options_schedule = None

		self.user = user
		self.pwd = pwd
		self.host = host
		self.db = db
		self.mysql_uri = MYSQL_URI.format(user, pwd, host, db)

		engine = create_engine(self.mysql_uri)
		if not database_exists(engine.url):
			create_database(engine.url)

		Base.metadata.create_all(engine, checkfirst=True)
		self.Session = sessionmaker(bind=engine)

	@contextmanager
	def session_scope(self):
		session = self.Session()
		try:
			yield session
			session.commit()
		except:
			session.rollback()
			raise
		finally:
			session.close()

	def add_to_schedule(self, obj):
		with self.session_scope() as session:
			if isinstance(obj, (list, tuple)):
				session.add_all(obj)
			else:
				session.add(obj)

	def create_options_task(self, symbol, trading_date):
		if not self.options_schedule:
			self.options_schedule = []
		self.options_schedule.append(OptionTask(symbol=symbol, trading_date=trading_date))

	def commit_options_tasks(self):
		self.add_to_schedule(self.options_schedule)

	def complete_options_task(self, symbol, trading_date):
		with self.session_scope() as session:
			task = session.query(OptionTask).filter_by(symbol=symbol, trading_date=trading_date).first()
			if not task:
				raise ScheduleDBError('OptionsTask does not exist: {} {}'.format(symbol, trading_date))
			task.completed = True
			session.add(task)
