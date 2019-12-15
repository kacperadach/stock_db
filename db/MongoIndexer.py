from pymongo import ASCENDING, DESCENDING

from core.StockDbBase import StockDbBase
from .Finance import FinanceDB
from .MongoIndex import MongoIndex

SYMBOL_EXCHANGE_COUNTRY_CODE_DATE_INTERVAL = MongoIndex(name='symbol_exchange_country_code_date_interval', index={'symbol': 1, 'exchange': 1, 'country_code': 1, 'time_interval': 1, 'trading_date': -1, }, unique=True)

COLLECTION_INDICES = {
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
    'scraper_stats': (MongoIndex(name='datetime_utc', index={'datetime_utc': -1}, unique=True, expire_after_seconds=1204800), ),
    'finviz': (MongoIndex(name='symbol_exchange_datetime', index={'symbol': 1, 'exchange': 1, 'datetime_utc': -1}, unique=False), ),
    'bonds': (MongoIndex(name='symbol_time_interval_trading_date', index={'symbol': 1, 'time_interval': 1, 'trading_date': -1}, unique=True), ),
    'fxstreet_economic': (MongoIndex(name='id', index={'id': 1}, unique=True),
                          MongoIndex(name='datetime_utc', index={'datetime_utc': 1}),
                          MongoIndex(name='event_id_datetime_utc', index={'event_id': 1, 'datetime_utc': 1}),
                          MongoIndex(name='country_code_datetime_utc', index={'country_code': 1, 'datetime_utc': 1})),
    'nasdaq_options': (MongoIndex(name='symbol_exchange_datetime_utc', index={'symbol': 1, 'exchange': 1, 'datetime_utc': 1}), ),
    'barchart_financials': (MongoIndex(name='symbol_exchange_document_type_period_date', index={'symbol': 1, 'exchange': 1, 'document_type': 1, 'period': 1, 'date': 1}, unique=True), )
}

# todo fix indexes
class MongoIndexer(StockDbBase):

    def __init__(self):
        super(MongoIndexer, self).__init__()
        self.finance_db = FinanceDB()

    def create_indices(self):
        self.log('Creating Indices')
        for collection, indices in COLLECTION_INDICES.items():
            for index in indices:
                try:
                    keys = [(k, ASCENDING if v == 1 else DESCENDING) for k, v in list(index.get_index().items())]
                    # keys = list(map(lambda k, v: (k, ASCENDING if v == 1 else DESCENDING), list(index.get_index().items())))
                    self.finance_db.create_index(collection, index.get_name(), keys, unique=index.get_unique(), expireAfterSeconds=index.get_expire_after_seconds())
                except Exception as e:
                    self.log_exception(e)
                    # raise RuntimeError('Error creating mongo indices')

if __name__ == "__main__":
    MongoIndexer().create_indices()
