from datetime import datetime

from core.StockDbBase import StockDbBase
from db.Finance import Finance_DB

COLLECTION_NAME = 'finviz'

class FinvizRepository(StockDbBase):

    def __init__(self):
        self.db = Finance_DB

    def insert(self, data, meta, utc_timestamp):
        document = {}
        document['symbol'] = meta['symbol']
        document['exchange'] = meta['exchange']
        document['datetime_utc'] = datetime.utcfromtimestamp(utc_timestamp)
        document['data'] = data
        self.db.insert(COLLECTION_NAME, document)

Finviz_Repository = FinvizRepository()