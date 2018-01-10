from datetime import datetime, time, timedelta
from pytz import timezone

from acquisition.symbol.financial_symbols import Financial_Symbols
from core.market.Market import is_market_open
from core.QueueItem import QueueItem
from db.Finance import Finance_DB
from request.YahooFinanceStockRequest import YahooFinanceStockRequest
from core.StockDbBase import StockDbBase

COLLECTION_NAME = 'stocks'
MAX_DAYS = 30
ONE_MIN_DAY_RANGE = 29
INTERVALS = ('1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h', '1d')

"""
Daily scraping 
"""

class StockScraper(StockDbBase):

    def __init__(self):
        super(StockScraper, self).__init__()
        self._reset()
        now = datetime.now(timezone('EST'))
        self.market_open = is_market_open(now)
        self.db = Finance_DB

    def _reset(self):
        self.counter = 0
        self.stock_tickers = Financial_Symbols.get_all()

    def get_next_input(self):
        now = datetime.now(timezone('EST'))
        market_open = is_market_open(now)
        if market_open and not self.market_open:
            self._reset()
            self.market_open = True
        if self.counter >= len(self.stock_tickers):
            if not market_open and self.market_open:
                self.market_open = False
                self._reset()
            elif not market_open and not self.market_open:
                return
            elif market_open and self.market_open:
                self._reset()

        current_ticker = self.stock_tickers[self.counter]
        if self.market_open:
            queue_item = self.get_live_queue_item(current_ticker)
            self.counter += 1
            return queue_item
        else:
            queue_item = self.get_historical_queue_item(current_ticker)
            self.counter += 1
            return queue_item

    def process_data(self, queue_item, request_queue):
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
                    self.db.replace_one(COLLECTION_NAME, {'symbol': document['symbol'], 'trading_date': document['trading_date']}, document)
        else:
            documents = self.get_documents_from_response(response)
            if not documents:
                return

            if documents[0]['time_interval'] != metadata['time_interval']:
                if metadata['time_interval'] == '1d':
                    return
            else:
                new_documents = []
                existing_documents = list(self.db.find(
                    COLLECTION_NAME,
                    {'symbol': metadata['symbol'], 'time_interval': metadata['time_interval'], 'trading_date': {'$gte': metadata['period1'] - timedelta(days=1), '$lte': metadata['period2']}},
                    {'data': 1, 'trading_date': 1}
                ))
                for document in documents:
                    existing_document = filter(lambda x: x['trading_date'] == document['trading_date'], existing_documents)
                    if not existing_document:
                        new_documents.append(document)
                    elif len(existing_document[0]['data']) < len(document['data']):
                        self.db.replace_one(COLLECTION_NAME, {'symbol': document['symbol'], 'time_interval': document['symbol'], 'trading_date': document['trading_date']}, document)
                if new_documents:
                    self.db.insert(COLLECTION_NAME, new_documents)

            market_open = is_market_open(datetime.now(timezone('EST')))
            if not market_open:
                if metadata['time_interval'] == '1m':
                    oldest_document = list(self.db.find(COLLECTION_NAME, {'symbol': metadata['symbol'], 'time_interval': '1d'}, {'trading_date': 1}).limit(1))
                    if oldest_document:
                        oldest_date = oldest_document[0]['trading_date']
                        period2 = oldest_date
                        period1 = period2 - timedelta(days=MAX_DAYS)
                    else:
                        yesterday_datetime = datetime.combine(datetime.now(timezone('EST')).date(), time()).replace(tzinfo=(timezone('EST'))) - timedelta(days=1)
                        period2 = yesterday_datetime
                        period1 = yesterday_datetime - timedelta(days=MAX_DAYS)
                else:
                    period2 = metadata['period1']
                    period1 = period2 - timedelta(days=MAX_DAYS)

                request_url = YahooFinanceStockRequest(symbol=metadata['symbol'], period1=period1, period2=period2, interval='1d').get_url()
                new_queue_item = QueueItem(symbol=metadata['symbol'], url=request_url, callback=self.process_data, metadata={'historical': True, 'symbol': metadata['symbol'], 'period1': period1, 'period2': period2, 'time_interval': '1d'})
                request_queue.put(new_queue_item)

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
        queue_item = QueueItem(symbol=current_ticker, url=request_url, callback=self.process_data, metadata={'symbol': current_ticker, 'historical': False, 'time_interval': '1m', 'period1': market_open_datetime, 'period2': now})
        return queue_item

    def get_historical_queue_item(self, current_ticker):
        today_datetime = datetime.combine(datetime.now(timezone('EST')).date(), time()).replace(tzinfo=timezone('EST'))
        request_url = YahooFinanceStockRequest(current_ticker, today_datetime - timedelta(days=ONE_MIN_DAY_RANGE), today_datetime, interval='1m').get_url()
        queue_item = QueueItem(symbol=current_ticker, url=request_url, callback=self.process_data, metadata={'symbol': current_ticker, 'historical': True, 'time_interval': '1m', 'period1': today_datetime - timedelta(days=ONE_MIN_DAY_RANGE), 'period2': today_datetime})
        return queue_item


