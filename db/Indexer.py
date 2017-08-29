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

from Finance import FinanceDB
from logger import Logger

class MongoIndexer():

    def __init__(self):
        self.task_name = 'MongoIndexer'
        self.finance_db = FinanceDB()

    def _log(self, msg, level='info'):
        Logger.log(msg, level, self.task_name)

    def create_indices(self):
        self._log('Creating Indices')
        for collection, keys in COLLECTION_INDICES.iteritems():
            self.finance_db.set_collection(collection)
            self.finance_db.create_index(keys)

if __name__ == "__main__":
    MongoIndexer().create_indices()