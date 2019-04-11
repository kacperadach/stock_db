from datetime import datetime

from pymongo import ASCENDING

from db.Finance import Finance_DB
from request.MarketWatchRequestConstants import INSTRUMENT_TYPES
from SnakeCase import SnakeCase

# normalize field names
FIELD_NAMES = (
    'time_interval',
    'utc_offset',
    'exchange',
    'symbol',
    'time_zone',
    'common_name',
    'data',
    'trading_date'
)

COLLECTION_NAME = "market_watch_"

class QuoteRepository():

    def __init__(self):
        self.db = Finance_DB

    def find(self, symbol, exchange, instrument_type, time_interval, trading_date):
        if not isinstance(trading_date, datetime):
            raise QuoteRepositoryException('trading_date must be datetime')
        if instrument_type not in INSTRUMENT_TYPES:
            raise QuoteRepositoryException('invalid instrument_type')
        collection = COLLECTION_NAME + instrument_type
        query = {'symbol': symbol, 'exchange': exchange, 'time_interval': time_interval, 'trading_date': trading_date}
        return self.db.find(collection, query, {})

    def find_all(self, symbol, exchange, instrument_type, time_interval, trading_dates, fields={}):
        if instrument_type not in INSTRUMENT_TYPES:
            raise QuoteRepositoryException('invalid instrument_type')
        collection = COLLECTION_NAME + instrument_type
        query = {
            'symbol': symbol,
            'trading_date': {'$in': trading_dates},
            'exchange': exchange,
            'time_interval': time_interval
        }
        return self.db.find(collection, query, fields)

    def insert(self, documents, instrument_type):
        if instrument_type not in INSTRUMENT_TYPES:
            raise QuoteRepositoryException('invalid instrument_type')
        collection = COLLECTION_NAME + instrument_type

        new_documents = []
        for d in documents:
            new_document = {}
            for k, v in d.iteritems():
                new_key = SnakeCase.to_snake_case(k)
                if new_key in FIELD_NAMES:
                    if k == 'symbol':
                        v = v.upper()
                    new_document[new_key] = v
            new_documents.append(new_document)

        self.db.insert(collection, new_documents)

class QuoteRepositoryException(Exception):
    pass

Quote_Repository = QuoteRepository()
