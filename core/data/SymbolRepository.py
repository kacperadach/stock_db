from datetime import datetime

from db.Finance import Finance_DB
from SnakeCase import SnakeCase

# normalize field names
FIELD_NAMES = (
    'sector',
    'instrument_type',
    'country_code',
    'exchange',
    'country',
    'symbol',
    'long_name'
)

COLLECTION_NAME = 'market_watch_symbols'

class SymbolRepository():

    def __init__(self):
        self.db = Finance_DB

    def find(self, symbol, exchange=None, instrument_type=None):
        query = {'symbol': symbol.upper()}
        if exchange is not None:
            query['exchange'] = exchange
        if instrument_type is not None:
            query['instrument_type'] = instrument_type
        return self.db.find(COLLECTION_NAME, query, {})

    def insert(self, documents):
        new_documents = []
        for d in documents:
            new_document = {}
            for k, v in d.iteritems():
                new_key = SnakeCase.to_snake_case(k)
                if new_key in FIELD_NAMES:
                    if k == 'symbol':
                        new_key = new_key.replace('$', '')
                        v = v.upper()
                    new_document[new_key] = v
            new_documents.append(new_document)

        self.db.insert(COLLECTION_NAME, new_documents)

class SymbolRepositoryException(Exception):
    pass

Symbol_Repository = SymbolRepository()
