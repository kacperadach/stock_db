from db import ScheduleDB

from acquisition.symbol import StockTickers

def schedule_options(trading_date):
	schedule_db = ScheduleDB()
	for ticker in StockTickers().get_all():
		schedule_db.create_options_task(ticker, trading_date)
	schedule_db.commit_options_tasks()

def complete_options(symbol, trading_date):
	schedule_db = ScheduleDB()
	schedule_db.complete_options_task(symbol, trading_date)