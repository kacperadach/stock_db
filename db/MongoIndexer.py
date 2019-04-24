from pymongo import ASCENDING, DESCENDING

from core.StockDbBase import StockDbBase
from Finance import Finance_DB
from MongoIndex import MongoIndex

COLLECTION_INDICES = {
    'stocks': (MongoIndex(name='symbol_date_interval', index={'symbol': 1, 'trading_date': -1, 'time_interval': 1}, unique=True),
                MongoIndex(name='symbol_interval', index={'symbol': 1, 'time_interval': 1}, unique=False),
                MongoIndex(name='symbol', index={'symbol': 1}, unique=False)),
    'symbols': (MongoIndex(name='symbol', index={'symbol': 1, 'region': 1}, unique=True),
                MongoIndex(name='fullExchangeName', index={'fullExchangeName': 1}, unique=False)),
    'forex_pairs': (MongoIndex(name='symbol', index={'symbol': 1}, unique=True),
                    MongoIndex(name='base', index={'base': 1}, unique=False),
                    MongoIndex(name='quote', index={'quote': 1}, unique=False)),
    'forex': (MongoIndex(name='symbol_date_interval', index={'symbol': 1, 'trading_date': -1, 'time_interval': 1}, unique=True),
                MongoIndex(name='symbol_interval', index={'symbol': 1, 'time_interval': 1}, unique=False),
                MongoIndex(name='symbol', index={'symbol': 1}, unique=False)),
    'futures_symbols': (MongoIndex(name='symbol', index={'symbol': 1}, unique=True), ),
    'market_watch_symbols': (MongoIndex(name='symbol', index={'symbol': 1, 'instrument_type': 1, 'exchange': 1}, unique=False),
                            MongoIndex(name='instrument_type', index={'instrument_type': 1}, unique=False),
                             MongoIndex(name='instrument_type_country', index={'instrument_type': 1, 'country': 1}, unique=False)),
    'market_watch_stocks': (MongoIndex(name='symbol_date_interval_exchange', index={'symbol': 1, 'exchange': 1, 'trading_date': -1, 'time_interval': 1}, unique=True),
                            MongoIndex(name='symbol_exchange', index={'symbol': 1, 'exchange': 1}, unique=False)),
    'market_watch_american-depository-receipt-stocks': (MongoIndex(name='symbol_date_interval', index={'symbol': 1, 'trading_date': -1, 'time_interval': 1}, unique=True), ),
    'market_watch_bonds': (MongoIndex(name='symbol_date_interval', index={'symbol': 1, 'trading_date': -1, 'time_interval': 1}, unique=True), ),
    'market_watch_currencies': (MongoIndex(name='symbol_date_interval', index={'symbol': 1, 'trading_date': -1, 'time_interval': 1}, unique=True), ),
    'market_watch_exchange-traded-funds': (MongoIndex(name='symbol_date_interval', index={'symbol': 1, 'trading_date': -1, 'time_interval': 1}, unique=True), ),
    'market_watch_exchange-traded-notes': (MongoIndex(name='symbol_date_interval', index={'symbol': 1, 'trading_date': -1, 'time_interval': 1}, unique=True), ),
    'market_watch_funds': (MongoIndex(name='symbol_date_interval', index={'symbol': 1, 'trading_date': -1, 'time_interval': 1}, unique=True), ),
    'market_watch_indexes': (MongoIndex(name='symbol_date_interval', index={'symbol': 1, 'trading_date': -1, 'time_interval': 1}, unique=True), ),
    'market_watch_rates': (MongoIndex(name='symbol_date_interval', index={'symbol': 1, 'trading_date': -1, 'time_interval': 1}, unique=True), ),
    'market_watch_real-estate-investment-trusts': (MongoIndex(name='symbol_date_interval', index={'symbol': 1, 'trading_date': -1, 'time_interval': 1}, unique=True), ),
    'market_watch_warrants': (MongoIndex(name='symbol_date_interval', index={'symbol': 1, 'trading_date': -1, 'time_interval': 1}, unique=True), ),
    'market_watch_request': (MongoIndex(name='instrument_type_exchange_symbol_timestamp', index={'instrument_type': 1, 'exchange': 1, 'symbol': 1, 'timestamp': 1}, unique=False), ),
}

class MongoIndexer(StockDbBase):

    def __init__(self):
        super(MongoIndexer, self).__init__()
        self.finance_db = Finance_DB

    def create_indices(self):
        try:
            self.log('Creating Indices')
            for collection, indices in COLLECTION_INDICES.iteritems():
                for index in indices:
                    keys = map(lambda (k,v): (k, ASCENDING if v == 1 else DESCENDING), index.get_index().iteritems())
                    self.finance_db.create_index(collection, index.get_name(), keys, unique=index.get_unique())
        except Exception as e:
            self.log_exception(e)

if __name__ == "__main__":
    MongoIndexer().create_indices()
