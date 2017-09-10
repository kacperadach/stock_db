import traceback

from pymongo import ASCENDING, DESCENDING

from Finance import Finance_DB
from logger import Logger

COLLECTION_INDICES = {
    'BioPharmCatalyst_fda': {'symbol': 1, 'date': -1, 'trading_date': -1},
    'BioPharmCatalyst_historical':  {'symbol': 1, 'date': -1, 'trading_date': -1},
    'commodities': {'meta.symbol': 1, 'symbol': 1, 'trading_date': -1},
    'currencies': {'meta.symbol': 1, 'symbol': 1, 'trading_date': -1},
    'reporting': {'trading_date': -1},
    'stock_historical': {'symbol': 1, 'trading_date': -1},
    'stock_insider': {'symbol': 1, 'trading_date': -1},
    'stock_options': {'symbol': 1, 'trading_date': -1}
}

class MongoIndexer():

    def __init__(self):
        self.task_name = 'MongoIndexer'
        self.finance_db = Finance_DB

    def _log(self, msg, level='info'):
        Logger.log(msg, level, self.task_name)

    def create_indices(self):
        try:
            self._log('Creating Indices')
            for collection, keys in COLLECTION_INDICES.iteritems():
                keys = map(lambda (k,v): (k, ASCENDING if v == 1 else DESCENDING), keys.iteritems())
                self.finance_db.create_index(keys, collection)
        except Exception as e:
            self._log("unexpected error occurred: {}".format(e), level='error')
            Logger.log(traceback.format_exc())

if __name__ == "__main__":
    MongoIndexer().create_indices()