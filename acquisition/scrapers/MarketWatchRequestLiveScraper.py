from datetime import timedelta

from acquisition.scrapers.MarketWatchHistoricalScraper import MarketWatchHistoricalScraper

REQUEST_COLLECTION = 'market_watch_request'

LIMIT = 100

REQUEST_FIELDS = {'instrument_type': 1, 'exchange': 1, 'symbol': 1}
FIELDS = {'symbol': 1, 'instrument_type': 1, 'exchange': 1, 'country': 1, 'country_code': 1}

LIVE_SCRAPE_PERIOD_SEC = 60

""" Looks at REQUEST_COLLECTION and scrapes those symbols """
class MarketWatchRequestLiveScraper(MarketWatchHistoricalScraper):

    def get_symbols(self):
        requests = self.db.find(REQUEST_COLLECTION, {}, REQUEST_FIELDS).sort('timestamp', -1).limit(LIMIT)
        requests = {request['instrument_type'] + request['exchange'] + request['symbol']: request for request in requests}
        cursor = []
        if requests.values():
            cursor = list(self.db.find(self.MARKET_WATCH_SYMBOL_COLLECTION, {'$or': list(requests.values())}, FIELDS))
        return iter(cursor)

    def get_time_delta(self):
        return timedelta(seconds=15)

    def process_data(self, queue_item):
        meta = queue_item.get_metadata()
        self.quote_repository.delete_request_quote(meta['symbol']['instrument_type'], meta['symbol']['exchange'], meta['symbol']['symbol'])
        super(MarketWatchRequestLiveScraper, self).process_data(queue_item=queue_item)