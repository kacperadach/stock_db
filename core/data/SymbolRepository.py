from datetime import datetime
from copy import deepcopy
import re
import base64

from core.data.uid import encrypt_unique_id
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

ALL_FIELDS = {x: 1 for x in FIELD_NAMES}

COLLECTION_NAME = 'market_watch_symbols'

class SymbolRepository():

    def __init__(self):
        self.db = Finance_DB

    def _get_all_fields(self):
        all_fields = deepcopy(ALL_FIELDS)
        all_fields['_id'] = False
        return all_fields

    def find(self, symbol, exchange=None, instrument_type=None, fields={}):
        query = {'symbol': symbol.upper()}
        if exchange is not None:
            query['exchange'] = exchange
        if instrument_type is not None:
            query['instrument_type'] = instrument_type
        return self.db.find(COLLECTION_NAME, query, fields)

    def get(self, symbol, exchange=None, instrument_type=None):
        symbol_list = list(self.find(symbol, exchange=exchange, instrument_type=instrument_type, fields=self._get_all_fields()))
        if symbol_list:
            return symbol_list[0]
        else:
            return {}

    def insert(self, documents):

        new_documents = []
        for d in documents:
            if 'country_code' not in d.iterkeys():
                self.db.find({'country': d['country']})

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

    def search(self, symbol_search):
        regex = re.compile('^'+ symbol_search.upper(), re.IGNORECASE)
        search_results = list(self.db.find(COLLECTION_NAME, {'symbol': regex}, self._get_all_fields()).sort((['symbol', 1], ['country', -1])).limit(5))
        for search_result in search_results:
            search_result['uid'] = encrypt_unique_id(search_result)

        return search_results

class SymbolRepositoryException(Exception):
    pass

Symbol_Repository = SymbolRepository()

if __name__ == '__main__':
    uid = Symbol_Repository.search('gdx')[0]['uid']
    print Symbol_Repository.decrypt_unique_id('QSAgICAvWE5ZUyAvVVMgIC9zdG9ja3MgICAg')
