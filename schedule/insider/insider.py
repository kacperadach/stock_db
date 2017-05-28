from db import ScheduleDB

from acquisition.symbol import StockTickers

def schedule_insider(trading_date):
    schedule_db = ScheduleDB()
    transactions = []
    for ticker in StockTickers().get_all():
        error = schedule_db.create_insider_task(ticker, trading_date)
        transactions.append({'error': error, 'ticker': ticker})
    return transactions

