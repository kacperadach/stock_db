from pymongo import ASCENDING, DESCENDING

from core.StockDbBase import StockDbBase
from Finance import Finance_DB
from MongoIndex import MongoIndex

SYMBOL_EXCHANGE_COUNTRY_CODE_DATE_INTERVAL = MongoIndex(name='symbol_exchange_country_code_date_interval', index={'symbol': 1, 'exchange': 1, 'country_code': 1, 'trading_date': -1, 'time_interval': 1}, unique=True)

COLLECTION_INDICES = {
    # 'stocks': (SYMBOL_EXCHANGE_COUNTRY_CODE_DATE_INTERVAL,
    #             MongoIndex(name='symbol_interval', index={'symbol': 1, 'time_interval': 1}, unique=False),
    #             MongoIndex(name='symbol', index={'symbol': 1}, unique=False)),
    # 'symbols': (MongoIndex(name='symbol', index={'symbol': 1, 'region': 1}, unique=True),
    #             MongoIndex(name='fullExchangeName', index={'fullExchangeName': 1}, unique=False)),
    # 'forex_pairs': (MongoIndex(name='symbol', index={'symbol': 1}, unique=True),
    #                 MongoIndex(name='base', index={'base': 1}, unique=False),
    #                 MongoIndex(name='quote', index={'quote': 1}, unique=False)),
    # 'forex': (MongoIndex(name='symbol_date_interval', index={'symbol': 1, 'trading_date': -1, 'time_interval': 1}, unique=True),
    #             MongoIndex(name='symbol_interval', index={'symbol': 1, 'time_interval': 1}, unique=False),
    #             MongoIndex(name='symbol', index={'symbol': 1}, unique=False)),
    # 'futures_symbols': (MongoIndex(name='symbol', index={'symbol': 1}, unique=True), ),
    'market_watch_symbols': (MongoIndex(name='symbol', index={'symbol': 1, 'instrument_type': 1, 'exchange': 1, 'country_code': 1}, unique=True),
                            MongoIndex(name='instrument_type', index={'instrument_type': 1}),
                             MongoIndex(name='instrument_type_country', index={'instrument_type': 1, 'country': 1}),
                             MongoIndex(name='symbol_only', index={'symbol': 1})),
    'market_watch_stocks': (SYMBOL_EXCHANGE_COUNTRY_CODE_DATE_INTERVAL,
                            MongoIndex(name='symbol_exchange', index={'symbol': 1, 'exchange': 1}, unique=False)),
    'market_watch_american-depository-receipt-stocks': (SYMBOL_EXCHANGE_COUNTRY_CODE_DATE_INTERVAL, ),
    'market_watch_bonds': (SYMBOL_EXCHANGE_COUNTRY_CODE_DATE_INTERVAL, ),
    'market_watch_currencies': (SYMBOL_EXCHANGE_COUNTRY_CODE_DATE_INTERVAL, ),
    'market_watch_exchange-traded-funds': (SYMBOL_EXCHANGE_COUNTRY_CODE_DATE_INTERVAL, ),
    'market_watch_exchange-traded-notes': (SYMBOL_EXCHANGE_COUNTRY_CODE_DATE_INTERVAL, ),
    'market_watch_funds': (SYMBOL_EXCHANGE_COUNTRY_CODE_DATE_INTERVAL, ),
    'market_watch_futures': (SYMBOL_EXCHANGE_COUNTRY_CODE_DATE_INTERVAL, ),
    'market_watch_indexes': (SYMBOL_EXCHANGE_COUNTRY_CODE_DATE_INTERVAL, ),
    'market_watch_rates': (SYMBOL_EXCHANGE_COUNTRY_CODE_DATE_INTERVAL, ),
    'market_watch_real-estate-investment-trusts': (SYMBOL_EXCHANGE_COUNTRY_CODE_DATE_INTERVAL, ),
    'market_watch_warrants': (SYMBOL_EXCHANGE_COUNTRY_CODE_DATE_INTERVAL, ),
    'market_watch_request': (MongoIndex(name='instrument_type_exchange_symbol_timestamp', index={'instrument_type': 1, 'exchange': 1, 'symbol': 1, 'timestamp': 1}, unique=False), ),
    'scraper_stats': (MongoIndex(name='datetime_utc', index={'datetime_utc': -1}, unique=True, expire_after_seconds=600), ) #604800
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
                    self.finance_db.create_index(collection, index.get_name(), keys, unique=index.get_unique(), expireAfterSeconds=index.get_expire_after_seconds())
        except Exception as e:
            self.log_exception(e)

if __name__ == "__main__":
    MongoIndexer().create_indices()
