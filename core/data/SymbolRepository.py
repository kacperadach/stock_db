from copy import deepcopy
import re

from core.StockDbBase import StockDbBase
from core.data.uid import encrypt_unique_id
from db.Finance import FinanceDB
from .SnakeCase import SnakeCase

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

class SymbolRepository(StockDbBase):

    def __init__(self):
        super(SymbolRepository, self).__init__()
        self.db = FinanceDB()

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
            new_document = {}
            for k, v in d.items():
                new_key = SnakeCase.to_snake_case(k)
                if new_key in FIELD_NAMES:
                    if k == 'symbol':
                        new_key = new_key.replace('$', '')
                        v = v.upper()
                    new_document[new_key] = v

            # if 'country_code' not in new_document.keys():
            #     self.db.find(COLLECTION_NAME, {'country': d['country']}, self._get_all_fields())

            new_documents.append(new_document)

        for document in new_documents:
            self.db.insert_one(COLLECTION_NAME, document)

    def search(self, symbol_search):
        regex = re.compile('^'+ symbol_search.upper(), re.IGNORECASE)
        search_results = list(self.db.find(COLLECTION_NAME, {'symbol': regex}, self._get_all_fields()).sort((['symbol', 1], ['country', -1])).limit(5))
        for search_result in search_results:
            search_result['uid'] = encrypt_unique_id(search_result)
        return search_results

    def delete_many(self, query):
        self.db.delete_many(COLLECTION_NAME, query)



class SymbolRepositoryException(Exception):
    pass

Symbol_Repository = SymbolRepository()

if __name__ == '__main__':
    uid = Symbol_Repository.search('gdx')[0]['uid']
    Symbol_Repository.decrypt_unique_id('QSAgICAvWE5ZUyAvVVMgIC9zdG9ja3MgICAg')
