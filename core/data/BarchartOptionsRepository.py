from copy import deepcopy
from datetime import datetime

from core.StockDbBase import StockDbBase
from db.Finance import FinanceDB

class BarchartOptionsRepository(StockDbBase):
    MARKET_WATCH_SYMBOL_COLLECTION = 'market_watch_symbols'
    BARCHART_OPTIONS_COLLECTION = 'barchart_options'

    def __init__(self):
        super(BarchartOptionsRepository, self).__init__()
        self.db = FinanceDB()

    def get_symbols(self):
        return self.db.find(self.MARKET_WATCH_SYMBOL_COLLECTION, {'country_code': 'US', 'instrument_type': {'$in': ['stocks', 'exchange-traded-funds', 'exchange-traded-notes']}}, {'symbol': 1, 'instrument_type': 1, 'exchange': 1, 'country_code': 1})

    def insert(self, data, metadata, utc_timestamp):
        document = deepcopy(metadata)
        del document['cookies']
        document['datetime_utc'] = datetime.utcfromtimestamp(utc_timestamp)
        document['expiration'] = data['expiration']
        document['is_monthly'] = data['is_monthly'][data['expiration'].strftime('%Y-%m-%d')]
        document['options'] = {
            'calls': data['calls'],
            'puts': data['puts']
        }
        self.db.insert(self.BARCHART_OPTIONS_COLLECTION, document)

