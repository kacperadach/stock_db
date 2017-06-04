from yfk.quote import Quote, QuoteError
from db import ScheduleDB, FinanceDB
from logger import Logger

def get_commodities_data(symbol):
    try:
        it = Quote(symbol)
        return it.get_data()
    except QuoteError:
        return {}

def get_all_commodities_data(trading_date):
    schedule_db = ScheduleDB()
    finance_db = FinanceDB('commodities')
    found, not_found = [], []
    for symbol in Logger.progress(schedule_db.get_incomplete_commodities_tasks(trading_date), 'commodities'):
        data = get_commodities_data(symbol)
        if data:
            data['trading_date'] = str(trading_date)
            finance_db.insert_one(data)
            schedule_db.complete_commodities_task(symbol, trading_date)
            found.append(symbol)
        else:
            not_found.append(symbol)
    return found, not_found
