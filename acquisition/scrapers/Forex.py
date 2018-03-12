from datetime import datetime, time, timedelta
from pytz import timezone

from bs4 import BeautifulSoup, SoupStrainer

from core.StockDbBase import StockDbBase
from core.QueueItem import QueueItem
from db.Finance import Finance_DB
from request.MarketWatchForexRequest import MarketWatchForexRequest
from request.MarketWatchRequestConstants import CURRENCY_PAIRS_URL

DATA_COLLECTION_NAME = 'forex'
CURRENCY_PAIR_COLLECTION_NAME = 'forex_pairs'
REAL_TIME_SCRAPE_MINUTE_FREQ = 5

class ForexScraper(StockDbBase):
    def __init__(self):
        super(ForexScraper, self).__init__()
        self.db = Finance_DB
        self._reset()

    def _reset(self):
        self.counter = 0
        self.currency_pairs = map(lambda x: x['symbol'], self.db.find(CURRENCY_PAIR_COLLECTION_NAME, {}, {'symbol': 1}))
        self.current_day = datetime.now(timezone('EST')).date()
        self.fetch_dict = {'attempted': 0, 'successful': 0}
        self.scrape_dict = {}

    def get_next_input(self):
        now = datetime.now(timezone('EST'))
        if now.date() > self.current_day:
            self._reset()

        if self.fetch_dict['attempted'] == self.fetch_dict['successful']:
            self.fetch_dict['attempted'] += 1
            return QueueItem(
                symbol='forex_pairs',
                url=CURRENCY_PAIRS_URL.format(self.fetch_dict['attempted']),
                http_method='GET',
                callback=self.process_data,
                metadata={'page': self.fetch_dict['attempted']}
            )

        if len(self.currency_pairs) == 0:
            self.currency_pairs = map(lambda x: x['symbol'], self.db.find(CURRENCY_PAIR_COLLECTION_NAME, {}, {'symbol': 1}))
            return

        if self.counter >= len(self.currency_pairs):
            self.counter = 0
            self.currency_pairs = map(lambda x: x['symbol'], self.db.find(CURRENCY_PAIR_COLLECTION_NAME, {}, {'symbol': 1}))

        currency_pair = self.currency_pairs[self.counter]
        self.counter += 1
        if currency_pair not in self.scrape_dict.keys():
            self.scrape_dict[currency_pair] = {'real_time': None, 'historical': False}

        if self.scrape_dict[currency_pair]['real_time'] is None or self.scrape_dict[currency_pair]['real_time'] < now - timedelta(minutes=REAL_TIME_SCRAPE_MINUTE_FREQ):
            self.scrape_dict[currency_pair]['real_time'] = now
            mwfr = MarketWatchForexRequest(currency_pair, '1m')
            return QueueItem(
                symbol=currency_pair,
                url = mwfr.get_url(),
                http_method=mwfr.get_http_method(),
                callback=self.process_data,
                headers=mwfr.get_headers()
            )
        elif self.scrape_dict[currency_pair]['historical'] is False:
            self.scrape_dict[currency_pair]['historical'] = True
            mwfr = MarketWatchForexRequest(currency_pair, '1d')
            return QueueItem(
                symbol=currency_pair,
                url=mwfr.get_url(),
                http_method=mwfr.get_http_method(),
                callback=self.process_data,
                headers=mwfr.get_headers()
            )

    def process_data(self, queue_item):
        if queue_item.get_symbol() == 'forex_pairs':
            documents = []
            try:
                bs = BeautifulSoup(queue_item.get_response().get_data(), 'lxml', parse_only = SoupStrainer('div', {'id': 'marketsindex'}))
                html_rows = bs.find('tbody').find_all('tr')
                pairs = map(lambda x: x.find('a').text, html_rows)
                for pair in pairs:
                    base, quote = '', ''
                    symbol = pair[pair.index('(') + 1: pair.index(')')]
                    if '/' in pair:
                        try:
                            base, quote = pair.split('(')[0].split('/')
                        except ValueError:
                            pass
                    documents.append({
                        'symbol': symbol,
                        'base': base.strip(),
                        'quote': quote.strip()
                    })
            except Exception as e:
                self.log_exception(e)
                raise e

            if documents:
                page = queue_item.get_metadata()['page']
                self.fetch_dict['successful'] = page
                all_pairs = list(self.db.find(CURRENCY_PAIR_COLLECTION_NAME, {}, {}).distinct('symbol'))
                new_documents = filter(lambda x: x['symbol'] not in all_pairs, documents)
                if new_documents:
                    self.db.insert(CURRENCY_PAIR_COLLECTION_NAME, new_documents)
        elif queue_item.get_response().status_code == 200:
            response = MarketWatchForexRequest.parse_response(queue_item.get_response().get_data())
            document_dict = {}
            for d in response['data']:
                date = d['datetime'].date()
                if date not in document_dict.keys():
                    document_dict[date] = {
                        'common_name': response['common_name'],
                        'symbol': response['symbol'],
                        'time_interval': response['time_interval'],
                        'timezone': response['time_zone'],
                        'trading_date': datetime.combine(date, time()),
                        'data': [d]
                    }
                else:
                    document = document_dict[date]
                    document['data'].append(d)
            documents = sorted(document_dict.values(), key=lambda x: x['trading_date'])

            min_date = min(documents, key=lambda x: x['trading_date'])['trading_date']
            max_date = max(documents, key=lambda x: x['trading_date'])['trading_date']
            if response['time_interval'] == '1m':
                existing_documents = list(self.db.find(
                        DATA_COLLECTION_NAME,
                        {'symbol': response['symbol'], 'time_interval': response['time_interval'], 'trading_date': {'$gte': min_date, '$lte': max_date}},
                        {'trading_date': 1, 'data': 1}
                ))
            else:
                existing_documents = list(self.db.find(
                    DATA_COLLECTION_NAME,
                    {'symbol': response['symbol'], 'time_interval': response['time_interval'], 'trading_date': {'$gte': min_date, '$lte': max_date}},
                    {'trading_date': 1}
                ))

            new_documents = []
            for document in documents:
                existing_document = filter(lambda x: x['trading_date'] == document['trading_date'], existing_documents)
                if not existing_document:
                    new_documents.append(document)
                elif response['time_interval'] == '1m' and len(existing_document[0]['data']) < len(document['data']):
                    self.log('replacing document')
                    self.db.replace_one(DATA_COLLECTION_NAME, {'symbol': document['symbol'], 'time_interval': document['symbol'], 'trading_date': document['trading_date']}, document)

            if new_documents:
                self.db.insert(DATA_COLLECTION_NAME, new_documents)
