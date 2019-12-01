from datetime import datetime

from core.StockDbBase import StockDbBase
from core.data.uid import decrypt_unique_id
from db.Finance import FinanceDB

COLLECTION_NAME = 'finviz'

FIELD_NAMES = (
    'datetime_utc',
    'exchange',
    'symbol',
    'data'
)

ALL_FIELDS = {x: 1 for x in FIELD_NAMES}

class FinvizRepository(StockDbBase):

    def __init__(self):
        super(FinvizRepository, self).__init__()
        self.db = FinanceDB()

    def insert(self, data, meta, utc_timestamp):
        document = {
            'symbol':  meta['symbol'],
            'exchange': meta['exchange'],
            'datetime_utc': datetime.utcfromtimestamp(utc_timestamp),
            'data': data
        }
        self.db.insert(COLLECTION_NAME, document)

    def get(self, uid):
        symbol = decrypt_unique_id(uid)
        if symbol['instrument_type'] not in ('stocks', 'exchange-traded-funds') or symbol['country_code'] != 'US':
            return {}

        d = list(self.db.find(COLLECTION_NAME, {'symbol': symbol['symbol'], 'exchange': symbol['exchange']}, ALL_FIELDS).sort('datetime_utc', -1).limit(1))
        data = {}
        if d:
            data = self._convert_for_api(d[0])

        return data

    def _convert_for_api(self, data):
        data['datetime_utc'] = str(data['datetime_utc'])

        for news in data['data']['news']:
            news['datetime'] = str(news['datetime'])

        for rating in data['data']['ratings']:
            rating['datetime'] = str(rating['datetime'])

        return data
