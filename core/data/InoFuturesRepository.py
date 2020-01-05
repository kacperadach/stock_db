from copy import deepcopy
from datetime import datetime

from db.Finance import FinanceDB

FUTURE_COLLECTION = 'ino_futures'
OPTIONS_COLLECTION = 'ino_futures_options'

class InoFuturesRepository:

    def __init__(self):
        self.db = FinanceDB()

    def get_oldest_future_date(self, contract, period):
        if period == 'history':
            period = 'day'
        return list(self.db.find(FUTURE_COLLECTION, {'contract': contract, 'period': period}, {'trading_date': 1}).sort('trading_date', 1).limit(1))

    def insert_future(self, data, meta):
        base = deepcopy(meta)
        del base['type']
        del base['start']
        del base['end']
        if base['period'] == 'history':
            base['period'] = 'day'

        base['contract_date'] = base['contract']['date']
        base['spread'] = base['contract']['spread']
        if base['contract']['spread_date']:
            base['spread_date'] = base['contract']['spread_date']

        base['name'] = base['contract']['name']
        base['contract'] = base['contract']['contract_link']

        date_dict = {}
        for d in data:
            if d['datetime_utc'].date() not in date_dict:
                date_dict[d['datetime_utc'].date()] = [d]
            else:
                date_dict[d['datetime_utc'].date()].append(d)

        # check existing
        for d in date_dict.items():
            document = deepcopy(base)
            document['data'] = d[1]
            document['trading_date'] = datetime.combine(d[0], datetime.min.time())
            self.db.replace_one(FUTURE_COLLECTION, {'contract': document['contract'], 'period': document['period'], 'trading_date': document['trading_date']}, document, upsert=True)

    def insert_option(self, data, meta):
        base = {}
        base['contract_date'] = meta['contract']['date']
        base['name'] = meta['contract']['name']
        base['contract'] = meta['contract']['contract_link']
        base['option_type'] = meta['option']['type']
        base['strike'] = meta['option']['strike']
        base['expiration'] = meta['option']['expiration']
        base['option'] = meta['option']['symbol_link']

        date_dict = {}
        for d in data:
            if d['datetime_utc'].date() not in date_dict:
                date_dict[d['datetime_utc'].date()] = [d]
            else:
                date_dict[d['datetime_utc'].date()].append(d)

        # check existing
        for d in date_dict.items():
            document = deepcopy(base)
            document['data'] = d[1]
            document['trading_date'] = datetime.combine(d[0], datetime.min.time())
            self.db.replace_one(OPTIONS_COLLECTION, {'option': document['option'], 'trading_date': document['trading_date']}, document, upsert=True)


