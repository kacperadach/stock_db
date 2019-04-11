from copy import deepcopy
from datetime import datetime, timedelta

import pymongo
from db.Finance import Finance_DB
from request.MarketWatchRequestConstants import *

COLLECTION = 'market_watch_{}'

class QuoteException(Exception):
    pass

class Quote():
    def __init__(self):
        self.db = Finance_DB
        self.instrument_type = dict((instrument, 1) for instrument in  INSTRUMENT_TYPES)

    def get(self, instrument_type, symbol, start, end, time_interval):
        self.verify_parameters(instrument_type, symbol, start, end, time_interval)
        collection = deepcopy(COLLECTION)
        collection = collection.format(instrument_type)
        # data = list(self.db.find(collection, {'symbol': symbol.upper(), 'time_interval': time_interval, 'trading_date': {'$gte': start.strftime("%Y-%m-%d"), '$lte': end.strftime("%Y-%m-%d")}}, {}).sort('trading_date', pymongo.ASCENDING))
        data = list(
            self.db.find(collection, {'symbol': symbol.upper(), 'time_interval': time_interval}, {}).sort(
                'trading_date', pymongo.ASCENDING))
        return data

    def verify_parameters(self, instrument_type, symbol, start, end, time_interval):
        if instrument_type not in self.instrument_type:
            raise QuoteException('invalid instrument_type')


if __name__ == '__main__':
    # print Quote().get('stocks', 'AAPL', datetime.today() - timedelta(days=5), datetime.today(), '1m')
    print Quote().get('stocks', 'AAPL', datetime.today() - timedelta(days=5), datetime.today(), '1m')