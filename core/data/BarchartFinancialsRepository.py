from copy import deepcopy
from datetime import datetime, date

from db.Finance import FinanceDB


FIELD_NAMES = (
    'date',
    'data',
    'country_code',
    'exchange',
    'symbol',
    'document_type',
    'period'
)

ALL_FIELDS = {x: 1 for x in FIELD_NAMES}

class BarchartFinancialsRepository:

    BARCHART_FINANCIALS_COLLECTION = 'barchart_financials'
    SYMBOLS_COLLECTION = 'market_watch_symbols'

    def __init__(self):
        self.db = FinanceDB()

    def get_symbols(self):
        return self.db.find(self.SYMBOLS_COLLECTION, {'instrument_type': 'stocks', 'country_code': 'US'}, {'symbol': 1, 'exchange': 1})

    def insert(self, data, metadata):
        base = deepcopy(metadata)
        del base['page']

        documents = []
        for d in data.items():
            document = deepcopy(base)
            document['date'] = datetime.combine(date(year=int(d[0].split('-')[1]), month=int(d[0].split('-')[0]), day=1), datetime.min.time())
            document['data'] = d[1]
            documents.append(document)

        existing_dict = {}
        for existing in self.db.find(self.BARCHART_FINANCIALS_COLLECTION, base, ALL_FIELDS):
            existing_dict[existing['date']] = existing

        for document in documents:
            if document['date'] in existing_dict.keys() and existing_dict[document['date']]['data'] == document['data']:
                continue

            self.db.replace_one(self.BARCHART_FINANCIALS_COLLECTION,
                                {'symbol': document['symbol'],
                                 'exchange': document['exchange'],
                                 'document_type': document['document_type'],
                                 'period': document['period'],
                                 'date': document['date']
                                 },
                                document,
                                upsert=True)
