from copy import deepcopy

from db.Finance import FinanceDB
import pytz

COLLECTION_NAME = 'ycharts_indicators'

class YchartsRepository:

    def __init__(self):
        self.db = FinanceDB()

    def insert(self, data, metadata):
        trading_dates = {}
        for d in data['data']:
            trading_dates[d['trading_date']] = d

        existing = list(self.db.find(COLLECTION_NAME, {'id': metadata['id'], 'trading_date': {'$in': list(trading_dates.keys())}}, {'trading_date': 1, 'value': 1}))
        documents_to_upsert = []
        for e in existing:
            date = e['trading_date'].replace(tzinfo=pytz.utc)
            if e['value'] != trading_dates[date]['value']:
                documents_to_upsert.append(trading_dates[date])
            del trading_dates[date]

        if not trading_dates:
            return

        meta = deepcopy(data)
        del meta['start']
        del meta['end']
        del meta['data']
        meta['id'] = metadata['id']
        meta['category'] = metadata['category']['category']

        documents = []
        for val in trading_dates.values():
            document = deepcopy(meta)
            document['trading_date'] = val['trading_date']
            document['value'] = val['value']
            documents.append(document)

        self.db.insert(COLLECTION_NAME, documents)

        for val in documents_to_upsert:
            document = deepcopy(meta)
            document['trading_date'] = val['trading_date']
            document['value'] = val['value']
            self.db.replace_one(COLLECTION_NAME, {'id': document['id'], 'trading_date': val['trading_date']}, document, upsert=True)
