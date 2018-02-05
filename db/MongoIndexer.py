from pymongo import ASCENDING, DESCENDING

from core.StockDbBase import StockDbBase
from Finance import Finance_DB
from MongoIndex import MongoIndex

COLLECTION_INDICES = {
    'stocks': (MongoIndex(name='symbol_date_interval', index={'symbol': 1, 'trading_date': -1, 'time_interval': 1}, unique=True), MongoIndex(name='symbol_interval', index={'symbol': 1, 'time_interval': 1}, unique=False)),
    'symbols': (MongoIndex(name='symbol', index={'symbol': 1}, unique=True), MongoIndex(name='fullExchangeName', index={'fullExchangeName': 1}, unique=False))
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
