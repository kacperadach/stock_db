from db import ScheduleDB

def schedule_tickers(trading_date):
	schedule_db = ScheduleDB()
	schedule_db.create_tickers_task(trading_date)

