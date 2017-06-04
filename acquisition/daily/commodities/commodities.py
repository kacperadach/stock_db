from datetime import datetime, timedelta

from yfk.quote import Quote, QuoteError
from db import ScheduleDB, FinanceDB
from logger import Logger

def get_commodities_data(symbol, trading_date):
    yesterday = trading_date - timedelta(days=1)
    period1 = datetime(year=yesterday.year, month=yesterday.month, day=yesterday.day, hour=0, minute=0)
    period2 = datetime(year=trading_date.year, month=trading_date.month, day=trading_date.day, hour=0, minute=0)
    try:
        it = Quote(symbol, period1=period1, period2=period2, interval='1m')
        return it.get_data()
    except QuoteError:
        return {}

def get_all_commodities_data(trading_date):
    schedule_db = ScheduleDB()
    finance_db = FinanceDB('commodities')
    found, not_found = [], []
    yesterday = trading_date - timedelta(days=1)
    yesterday = datetime(year=yesterday.year, month=yesterday.month, day=yesterday.day)
    for symbol in Logger.progress(schedule_db.get_incomplete_commodities_tasks(yesterday), 'commodities'):
        data = get_commodities_data(symbol)
        if data:
            data['trading_date'] = str(trading_date)
            finance_db.insert_one(data)
            schedule_db.complete_commodities_task(symbol, trading_date)
            found.append(symbol)
        else:
            not_found.append(symbol)
    return found, not_found
