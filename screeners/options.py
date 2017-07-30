from datetime import datetime, timedelta

import numpy as np

from db.Finance import FinanceDB

OPTIONS_VALUE_KEYS = ('ask', 'bid', 'change', 'impliedVolatility', 'lastPrice', 'openInterest', 'percentChange', 'volume')

def run(ticker, target_day, data_parser, max_period=30):
    fb = FinanceDB('stock_options')
    options_data_gen = fb.find({"underlyingSymbol": ticker})

    data_array = []
    for day in options_data_gen:
        if len(data_array) >= max_period:
            break
        data_array.append(data_parser(day))

    target_day_data = fb.find({"underlyingSymbol": ticker, "trading_date": target_day}).next()
    target_day_data = data_parser(target_day_data)

    for expiration_date, strikes in target_day_data.iteritems():
        for strike in strikes.iterkeys():
            data = []
            for day_data in data_array:
                try:
                    data.append(day_data[expiration_date][strike])
                except:
                    pass
            data = np.array(data)
            averages = data.mean(0)
            std = data.std(0)
            tdd = target_day_data[expiration_date][strike]
            pass


def options_data_parser(day_data):
    data_array = {}
    for key, value in day_data['options'].iteritems():
        data_array[key] = {}
        for type in ('calls', 'puts'):
            options_type = {}
            for options in value[type]:
                options_type[options['strike']] = [options[k] for k in OPTIONS_VALUE_KEYS]
            data_array[key] = options_type
    return data_array

if __name__ == "__main__":
    target = datetime(year=2017, month=6, day=23).date()
    run('AAPL', str(target), options_data_parser)