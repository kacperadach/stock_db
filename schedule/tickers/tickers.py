from db import ScheduleDB
from db.models.schedule import TickerTask

def schedule_tickers(trading_date):
	schedule_db = ScheduleDB()
	schedule_db.create_tickers_task(trading_date)

def complete_tickers(trading_date):
	schedule_db = ScheduleDB()
	schedule_db.complete_tickers_task(trading_date)

def tickers_task_complete(trading_date):
	schedule_db = ScheduleDB()
	ticker_task = schedule_db.query(TickerTask, {'trading_date': trading_date}).first()
	return ticker_task.completed if hasattr(ticker_task, 'completed') else False