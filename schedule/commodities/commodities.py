from db import ScheduleDB

from acquisition.symbol import StockTickers

def schedule_commodities(trading_date):
	schedule_db = ScheduleDB()
	transactions = []
	for ticker in StockTickers().get_commodities():
		error = schedule_db.create_commodities_task(ticker, trading_date)
		transactions.append({'error': error, 'ticker': ticker})
	return transactions
