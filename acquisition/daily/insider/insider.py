from yfk.insider_transactions import InsiderTransactions, InsiderError
from db import ScheduleDB, FinanceDB

def get_insider_data(symbol):
    try:
        it = InsiderTransactions(symbol)
        return it.get_data()
    except InsiderError:
        return {}

def get_all_insider_data(trading_date):
    schedule_db = ScheduleDB()
    finance_db = FinanceDB('stock_insider')
    found, not_found = [], []
    for symbol in schedule_db.get_incomplete_insider_tasks(trading_date):
        data = get_insider_data(symbol)
        if data:
            finance_db.insert_one(data)
            schedule_db.complete_options_task(symbol, trading_date)
            found.append(symbol)
        else:
            not_found.append(symbol)
    return found, not_found