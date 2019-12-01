from datetime import timedelta, datetime

from pytz import timezone

from core.BaseScraper import BaseScraper
from core.QueueItem import QueueItem
from core.data.BondRepository import BondRepository
from request.BondRequest import BondRequest, BONDS, BOND_COUNTRY_CODES


class BondScraper(BaseScraper):

    def __init__(self):
        super(BondScraper, self).__init__()
        self.bond_repository = BondRepository()

    def get_symbols(self):
        for bond_type in BONDS.items():
            for bond in bond_type[1].items():
                yield {
                    'symbol': bond[0],
                    'name': bond[1],
                    'country_code': BOND_COUNTRY_CODES[bond_type[0]]
                }

    def get_queue_item(self, symbol):
        now = datetime.now(timezone('EST'))

        start = now - timedelta(days=20000)
        most_recent = self.bond_repository.get_most_recent_trading_date(symbol['symbol'], '1D')
        if most_recent is not None:
            start = most_recent

        request = BondRequest(symbol['symbol'], '1D', start, now)
        symbol['time_interval'] = '1D'
        return QueueItem(
            url=request.get_url(),
            headers=request.get_headers(),
            http_method=request.get_http_method(),
            callback=__name__,
            metadata=symbol
        )

    def get_time_delta(self):
        return timedelta(days=1)

    def process_data(self, queue_item):
        if not queue_item.get_response().is_successful():
            return

        data = BondRequest.parse_response(queue_item.get_response().get_data())
        if data:
            self.bond_repository.insert(data, queue_item.get_metadata())

