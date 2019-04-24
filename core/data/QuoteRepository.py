import time
from datetime import datetime
from copy import deepcopy
from dateutil import tz

from pymongo import ASCENDING

from core.StockDbBase import StockDbBase
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

ALL_FIELDS = {x: 1 for x in FIELD_NAMES}

TIME_INTERVALS = (
    '1m',
    '1d'
)

COLLECTION_NAME = 'market_watch_'
REQUEST_COLLECTION  = 'market_watch_request'

class QuoteRepository(StockDbBase):

    def __init__(self):
        self.db = Finance_DB

    def _get_all_fields(self):
        all_fields = deepcopy(ALL_FIELDS)
        all_fields['_id'] = False
        return all_fields

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


    def request_quote(self, instrument_type, exchange, symbol):
        self.log('Requested quote: {}/{}/{}'.format(instrument_type, exchange, symbol))
        self.db.replace_one(REQUEST_COLLECTION,
                            {'instrument_type': instrument_type, 'exchange': exchange, 'symbol': symbol},
                            {'instrument_type': instrument_type, 'exchange': exchange, 'symbol': symbol, 'timestamp': time.time()},
                            upsert=True)

    def delete_request_quote(self, instrument_type, exchange, symbol):
        self.db.delete_many(REQUEST_COLLECTION, {'instrument_type': instrument_type, 'exchange': exchange, 'symbol': symbol})

    def get_interval(self, instrument_type, exchange, symbol, start, end, time_interval):
        if instrument_type not in INSTRUMENT_TYPES:
            raise QuoteRepositoryException('invalid instrument_type')
        if  not isinstance(start, datetime):
            raise QuoteRepositoryException('start must be datetime')
        if  not isinstance(end, datetime):
            raise QuoteRepositoryException('end must be datetime')
        collection = COLLECTION_NAME + instrument_type
        return self._convert_for_chart(list(self.db.find(
            collection,
            {'symbol': symbol, 'exchange': exchange, 'time_interval': time_interval, 'trading_date': {'$lte': end, '$gte': start}},
            self._get_all_fields()
        ).sort('trading_date', -1)))

    def _convert_for_chart(self, data):
        new_data = []
        meta_data = {}

        time_interval = None;
        for d in data:
            if not meta_data:
                meta_data = deepcopy(d)
                time_interval = meta_data['time_interval']
                del meta_data['data']
                del meta_data['trading_date']
            for temp in reversed(d['data']):
                temp_data = {'macd': {}}
                for k, v in temp.iteritems():
                    if k.lower() == 'datetime':
                        temp_data['date'] = v.strftime('%Y-%m-%d %H:%M:%S')
                    elif k.lower() == 'last':
                        temp_data['close'] = v
                    elif k.lower() == 'macd':
                        temp_data['macd']['macd'] = v
                    elif k.lower() == 'macd-hist':
                        temp_data['macd']['divergence'] = v
                    elif k.lower() == 'macd-signal':
                        temp_data['macd']['signal'] = v
                    else:
                        temp_data[k.lower()] = v
                new_data.append(temp_data)
        new_data.reverse()
        return {'meta_data': meta_data, 'data': new_data}



class QuoteRepositoryException(Exception):
    pass

Quote_Repository = QuoteRepository()
