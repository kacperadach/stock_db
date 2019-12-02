from db.Finance import FinanceDB

FXSTREET_COLLECTION = 'fxstreet_economic'

class FxstreetRepository:

    def __init__(self):
        self.db = FinanceDB()

    def get_most_recent_event_date(self):
        try:
            return next(self.db.find(FXSTREET_COLLECTION, {}, {'datetime_utc': 1}).sort('datetime_utc', -1).limit(1))['datetime_utc']
        except StopIteration:
            return None

    def insert(self, data):
        for d in data:
            self.db.replace_one(FXSTREET_COLLECTION, {'id': d['id']}, d, upsert=True)
