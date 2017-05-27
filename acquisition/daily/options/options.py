from yfk.options import Options, OptionsError
from db import ScheduleDB, FinanceDB

def get_options_data(symbol):
    try:
        o = Options(symbol, auto_query=True)
        return o.get_data()
    except OptionsError:
        return {}


def get_all_options_data(trading_date):
    schedule_db = ScheduleDB()
    finance_db = FinanceDB('stock_options')
    not_found = []
    found = []
    for symbol in schedule_db.get_incomplete_options_tasks(trading_date):
        try:
            data = get_options_data(symbol)
            if data['options']:
                options = {}
                for key in data['options'].keys():  # convert int keys into str
                    options[str(key)] = data['options'][key]
                data['options'] = options
                finance_db.insert_one(data)
                schedule_db.complete_options_task(symbol, trading_date)
                found.append(symbol)
        except OptionsError:
            not_found.append(symbol)
    return found, not_found

