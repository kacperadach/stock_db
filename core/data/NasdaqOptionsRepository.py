from copy import deepcopy
from datetime import datetime

from core.StockDbBase import StockDbBase
from db.Finance import FinanceDB

class NasdaqOptionsRepository(StockDbBase):
    MARKET_WATCH_SYMBOL_COLLECTION = 'market_watch_symbols'
    NASDAQ_OPTIONS_COLLECTION = 'nasdaq_options'

    def __init__(self):
        super(NasdaqOptionsRepository, self).__init__()
        self.db = FinanceDB()

    def get_symbols(self):
        return self.db.find(self.MARKET_WATCH_SYMBOL_COLLECTION, {'country_code': 'US', 'instrument_type': {'$in': ['stocks', 'exchange-traded-funds', 'exchange-traded-notes']}}, {'symbol': 1, 'instrument_type': 1, 'exchange': 1, 'country_code': 1})

    def insert(self, options, metadata, utc_timestamp):
        document = deepcopy(metadata)
        document['options'] = options
        document['datetime_utc'] = datetime.utcfromtimestamp(utc_timestamp)
        self.db.insert(self.NASDAQ_OPTIONS_COLLECTION, document)

