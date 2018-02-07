from datetime import datetime, time, timedelta
from pytz import timezone

from core.market.Market import is_market_open
from core.QueueItem import QueueItem
from db.Finance import Finance_DB
from request.YahooFinanceStockRequest import YahooFinanceStockRequest
from core.StockDbBase import StockDbBase

COLLECTION_NAME = 'stocks'
MAX_DAYS = 30
ONE_MIN_DAY_RANGE = 29
INTERVALS = ('1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h', '1d')
MAX_QUEUE_SIZE = 1000
HISTORICAL_RETRY = 3

"""
Daily Stock Scraping
+------------------------------------------------------------------+
Market Open:
    - Scrape from 9:30am to current time
Market Closed:
    - Grab last 30 days 1m data
    - Then find oldest document and scrape MAX_DAYS in the past
        - if more data is found repeat
        - built-in HISTORICAL_RETRY
"""

class StockScraper(StockDbBase):

    def __init__(self):
        super(StockScraper, self).__init__()
        self.db = Finance_DB
        self._reset()
        self.market_open = is_market_open(datetime.now(timezone('EST')))
        self.historical_dict = {}

    def _reset(self):
        self.counter = 0
        self.stock_tickers = map(lambda x: x['symbol'], self.db.find('symbols', {}, {'symbol': 1}))

    def get_next_input(self):
        now = datetime.now(timezone('EST'))
        market_open = is_market_open(now)
        if market_open and not self.market_open:
            self._reset()
            self.market_open = True
            self.historical_dict = {}
        if self.counter >= len(self.stock_tickers):
            if not market_open and self.market_open:
                self.market_open = False
                self._reset()
                self.historical_dict = {}
            elif not market_open and not self.market_open:
                self._reset()
                return
            elif market_open and self.market_open:
                self._reset()
                self.historical_dict = {}

        current_ticker = self.stock_tickers[self.counter]
        if self.market_open:
            queue_item = self.get_live_queue_item(current_ticker)
            self.counter += 1
            return queue_item
        else:
            queue_item = self.get_historical_queue_item(current_ticker)
            self.counter += 1
            return queue_item

    def process_data(self, queue_item):
        response = queue_item.get_response()
        if not response or response.status_code != 200:
            return

        response = YahooFinanceStockRequest.parse_response(queue_item.get_response().get_data())
        metadata = queue_item.get_metadata()
        if metadata['historical'] is False:
            documents = self.get_documents_from_response(response)
            if documents:
                document = documents[0]
                if document['time_interval'] == '1m':
                    existing_document = list(self.db.find(COLLECTION_NAME, {'symbol': queue_item.get_symbol(), 'time_interval': '1m', 'trading_date': document['trading_date']}, {}).limit(1))
                    if existing_document:
                        self.db.replace_one(COLLECTION_NAME, {'symbol': document['symbol'], 'time_interval': '1m', 'trading_date': document['trading_date']}, document)
                    else:
                        self.db.insert(COLLECTION_NAME, document)
        else:
            documents = self.get_documents_from_response(response)
            if not documents:
                return

            if documents[0]['time_interval'] != metadata['time_interval']:
                return

            min_date =  min(documents, key=lambda x: x['trading_date'])['trading_date']
            max_date = max(documents, key=lambda x: x['trading_date'])['trading_date']

            new_documents = []
            existing_documents = list(self.db.find(
                COLLECTION_NAME,
                {'symbol': metadata['symbol'], 'time_interval': metadata['time_interval'], 'trading_date': {'$gte': min_date, '$lte': max_date}},
                {'data': 1, 'trading_date': 1}
            ))
            for document in documents:
                existing_document = filter(lambda x: x['trading_date'] == document['trading_date'], existing_documents)
                if not existing_document:
                    new_documents.append(document)
                elif len(existing_document[0]['data']) < len(document['data']):
                    self.log("replacing document")
                    self.db.replace_one(COLLECTION_NAME, {'symbol': document['symbol'], 'time_interval': document['symbol'], 'trading_date': document['trading_date']}, document)
            if new_documents:
                self.db.insert(COLLECTION_NAME, new_documents)

    def get_documents_from_response(self, response):
        document_dict = {}
        data = response['data']
        for d in data:
            date = d[0].date()
            if date not in document_dict.keys():
                document_dict[date] = {
                    'symbol': response['meta']['symbol'],
                    'trading_date': datetime.combine(date, time()),
                    'time_interval': response['meta']['dataGranularity'],
                    'meta': response['meta'],
                    'data': [d]
                }
            else:
                document = document_dict[date]
                document['data'].append(d)
        return sorted(document_dict.values(), key=lambda x: x['trading_date'])

    def get_live_queue_item(self, current_ticker):
        now = datetime.now(timezone('EST'))
        market_open_datetime = datetime(year=now.year, month=now.month, day=now.day, hour=9, minute=30, tzinfo=timezone('EST'))
        request_url = YahooFinanceStockRequest(current_ticker, market_open_datetime, now, interval='1m').get_url()
        queue_item = QueueItem(symbol=current_ticker, url=request_url, http_method='GET', callback=self.process_data, metadata={'symbol': current_ticker, 'historical': False, 'time_interval': '1m', 'period1': market_open_datetime, 'period2': now})
        return queue_item

    def get_historical_queue_item(self, current_ticker):
        if current_ticker not in self.historical_dict.keys():
            self.historical_dict[current_ticker] = None
            today_datetime = datetime.combine(datetime.now(timezone('EST')).date(), time()).replace(tzinfo=timezone('EST'))
            request_url = YahooFinanceStockRequest(current_ticker, today_datetime - timedelta(days=ONE_MIN_DAY_RANGE), today_datetime, interval='1m').get_url()
            queue_item = QueueItem(symbol=current_ticker, url=request_url, http_method='GET', callback=self.process_data, metadata={'symbol': current_ticker, 'historical': True, 'time_interval': '1m', 'period1': today_datetime - timedelta(days=ONE_MIN_DAY_RANGE), 'period2': today_datetime})
            return queue_item
        elif self.historical_dict[current_ticker] is None:
            oldest_document = list(self.db.find(COLLECTION_NAME, {'symbol': current_ticker, 'time_interval': '1d'}, {'trading_date': 1}).limit(1))
            if oldest_document:
                oldest_date = oldest_document[0]['trading_date']
                period2 = oldest_date.replace(tzinfo=timezone('EST'))
                period1 = period2 - timedelta(days=MAX_DAYS)
            else:
                period2 = datetime.combine(datetime.now(timezone('EST')).date(), time()).replace(tzinfo=timezone('EST'))
                period1 = period2 - timedelta(days=MAX_DAYS)
            self.historical_dict[current_ticker] = [period2, 1]
            request_url = YahooFinanceStockRequest(symbol=current_ticker, period1=period1, period2=period2, interval='1d').get_url()
            return QueueItem(symbol=current_ticker, url=request_url, http_method='GET', callback=self.process_data, metadata={'historical': True, 'symbol': current_ticker, 'period1': period1, 'period2': period2, 'time_interval': '1d'})
        else:
            oldest_document = list(self.db.find(COLLECTION_NAME, {'symbol': current_ticker, 'time_interval': '1d'}, {'trading_date': 1}).sort('trading_date', 1).limit(1))
            if not oldest_document:
                return None
            oldest_date = oldest_document[0]['trading_date'].replace(tzinfo=timezone('EST'))
            if not oldest_date < self.historical_dict[current_ticker][0]:
                if self.historical_dict[current_ticker][1] > HISTORICAL_RETRY:
                    return None
                else:
                    self.log('RETRYING')
                    self.historical_dict[current_ticker][1] = self.historical_dict[current_ticker][1] + 1
            period2 = oldest_date.replace(tzinfo=timezone('EST'))
            period1 = period2 - timedelta(days=MAX_DAYS)
            self.historical_dict[current_ticker][0] = period2
            request_url = YahooFinanceStockRequest(symbol=current_ticker, period1=period1, period2=period2, interval='1d').get_url()
            return QueueItem(symbol=current_ticker, url=request_url, http_method='GET', callback=self.process_data, metadata={'historical': True, 'symbol': current_ticker, 'period1': period1, 'period2': period2, 'time_interval': '1d'})
