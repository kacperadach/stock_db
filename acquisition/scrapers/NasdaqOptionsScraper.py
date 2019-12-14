from core.BaseScraper import BaseScraper
from datetime import datetime, timedelta
import pytz

from core.QueueItem import QueueItem
from core.data.NasdaqOptionsRepository import NasdaqOptionsRepository
from request.NasdaqOptionsRequest import NasdaqOptionsRequest


class NasdaqOptionsScraper(BaseScraper):

    def __init__(self):
        super(NasdaqOptionsScraper, self).__init__()
        self.nasdaq_options_repository = NasdaqOptionsRepository()

    def get_symbols(self):
        return iter(list(self.nasdaq_options_repository.get_symbols()))

    def get_queue_item(self, symbol):
        request = NasdaqOptionsRequest(symbol['symbol'], symbol['instrument_type'])
        return QueueItem(url=request.get_url(), headers=request.get_headers(), http_method=request.get_http_method(), callback=__name__, metadata=symbol)

    def get_time_delta(self):
        return timedelta(hours=4)

    def should_scrape(self):
        now = pytz.timezone('UTC').localize(datetime.utcnow())
        est = pytz.timezone('US/Eastern')
        est_now = now.astimezone(est)

        scrape_date = est_now.date()
        if est_now.hour <= 5:
            scrape_date = (est_now - timedelta(days=1)).date()

        last_scrape_date = self.last_scrape.date()
        if self.last_scrape.hour <= 5 and self.last_scrape.year > 1:
            last_scrape_date = (self.last_scrape - timedelta(days=1)).date()

        if scrape_date <= last_scrape_date:
            return False

        return est_now.hour <= 5 or est_now.hour >= 19

    def process_data(self, queue_item):
        if not queue_item.get_response().is_successful():
            return

        data = NasdaqOptionsRequest.parse_response(queue_item.get_response().get_data())
        if any(data.values()):
            self.nasdaq_options_repository.insert(data, queue_item.get_metadata(), queue_item.get_utc_time())

if __name__ == '__main__':
    now = pytz.timezone('UTC').localize(datetime.utcnow())
    est = pytz.timezone('US/Eastern')
    est_now = now.astimezone(est)
    a = 1