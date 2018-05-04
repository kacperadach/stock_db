from copy import deepcopy

from db.Finance import Finance_DB

COLLECTION = 'market_watch_{}'

class Quote():
    def __init__(self):
        self.db = Finance_DB

    def get(self, instrument_type, symbol, start, end, time_interval):
        collection = deepcopy(COLLECTION)
        collection = collection.format(instrument_type)
        data = list(self.db.find(collection, {'symbol': symbol, 'time_interval': time_interval, 'trading_date': {'$gte': start.strftime("%Y-%m-%d"), '$lte': end.strftime("%Y-%m-%d")}}, {}))
        return data
