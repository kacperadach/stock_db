from datetime import datetime

from db.Finance import FinanceDB

BOND_COLLECTION = 'bonds'


class BondRepository:

    def __init__(self):
        self.db = FinanceDB()

    def insert(self, data, meta_data):
        documents = []

        existing = self.get_existing_trading_dates(meta_data['symbol'], meta_data['time_interval'])

        for d in data:
            if len(d) == 0:
                continue

            trading_date = datetime.combine(d[0]['datetime_utc'].date(), datetime.min.time())
            if trading_date in existing:
                continue

            document = {'data': d}
            document['trading_date'] = trading_date
            document['symbol'] = meta_data['symbol']
            document['time_interval'] = meta_data['time_interval']
            document['country_code'] = meta_data['country_code']
            documents.append(document)

        if documents:
            self.db.insert(BOND_COLLECTION, documents)

    def get_existing_trading_dates(self, symbol, time_interval):
        return list(map(lambda x: x['trading_date'], self.db.find(BOND_COLLECTION, {'symbol': symbol, 'time_interval': time_interval}, {'trading_date': 1})))

    def get_most_recent_trading_date(self, symbol, time_interval):
        try:
            return next(self.db.find(BOND_COLLECTION, {'symbol': symbol, 'time_interval': time_interval}, {'trading_date': -1}).limit(1))['trading_date']
        except StopIteration:
            return None
