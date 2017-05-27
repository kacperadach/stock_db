from db import ScheduleDB

from acquisition.symbol import StockTickers

def schedule_options(trading_date):
	schedule_db = ScheduleDB()
	transactions = []
	for ticker in StockTickers().get_all():
		error = schedule_db.create_options_task(ticker, trading_date)
		transactions.append({'error': error, 'ticker': ticker})
	return transactions

def complete_options(symbol, trading_date):
	schedule_db = ScheduleDB()
	schedule_db.complete_options_task(symbol, trading_date)
