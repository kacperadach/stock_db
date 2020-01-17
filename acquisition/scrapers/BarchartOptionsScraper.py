from datetime import timedelta, datetime
from copy import deepcopy

import pytz

from core.BaseScraper import BaseScraper
from core.QueueItem import QueueItem
from core.data.BarchartOptionsRepository import BarchartOptionsRepository
from request.BarchartOptionsRequest import BarchartAuthRequest, BarchartOptionsRequest, AUTH_COOKIES


class BarchartOptionsScraper(BaseScraper):

    def __init__(self):
        super(BarchartOptionsScraper, self).__init__()
        self.barchart_options_repository = BarchartOptionsRepository()

    def get_symbols(self):
        return iter(list(self.barchart_options_repository.get_symbols()))


    def get_queue_item(self, symbol):
        if 'expiration' in symbol.keys():
            cookies = symbol['cookies']
            request = BarchartOptionsRequest(symbol['symbol'], cookies['XSRF-TOKEN'], cookies['laravel_session'], cookies['market'], symbol['expiration'])
            return QueueItem.from_request(request, __name__, symbol)
        elif 'cookies' in symbol.keys():
            cookies = symbol['cookies']
            request = BarchartOptionsRequest(symbol['symbol'], cookies['XSRF-TOKEN'], cookies['laravel_session'], cookies['market'])
            return QueueItem.from_request(request, __name__, symbol)
        else:
            request = BarchartAuthRequest()
            return QueueItem.from_request(request, __name__, symbol)

    def get_time_delta(self):
        return timedelta(hours=4)

    def process_data(self, queue_item):
        meta = queue_item.get_metadata()
        if 'cookies' not in meta:
            return

        data = BarchartOptionsRequest.parse_response(queue_item.get_response().get_data())
        if any(data.values()):
            self.barchart_options_repository.insert(data, queue_item.get_metadata(), queue_item.get_utc_time())

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

    def requests_per_second(self):
        return 5

    def request_callback(self, queue_item):
        # rate limited
        if queue_item.get_response().status_code == 429:
            self.additional_symbols.append(queue_item.get_metadat())

        if 'cookies' not in queue_item.get_metadata():
            cookies = BarchartAuthRequest.parse_response(queue_item.get_response())
            meta = deepcopy(queue_item.get_metadata())
            if all(k in cookies for k in AUTH_COOKIES):
                meta['cookies'] = cookies
            self.additional_symbols.append(meta)
        elif 'expiration' not in queue_item.get_metadata():
            data = BarchartOptionsRequest.parse_response(queue_item.get_response().get_data())
            if 'expirations' in data.keys():
                for expiration in data['expirations']:
                    if expiration == datetime.strftime(data['expiration'], '%Y-%m-%d'):
                        continue
                    meta = deepcopy(queue_item.get_metadata())
                    meta['expiration'] = expiration
                    self.additional_symbols.append(meta)
