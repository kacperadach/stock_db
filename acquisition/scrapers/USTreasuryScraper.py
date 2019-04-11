from datetime import datetime
from pytz import timezone

from core.StockDbBase import StockDbBase
from db.Finance import Finance_DB
from request.TreasuryRequest import TreasuryRequest, TYPES
from core.QueueItem import QueueItem

class USTreasuryScraper(StockDbBase):
    def __init__(self):
        super(USTreasuryScraper, self).__init__()
        self.db = Finance_DB
        self.scrape_dict = {}
        self.today = datetime.now(timezone('EST'))

    def get_next_input(self):
        now = datetime.now(timezone('EST'))

        if now.date() != self.today.date():
            self.today = now
            self.scrape_dict = {}

        for type in TYPES:
            if type not in self.scrape_dict.keys():
                self.scrape_dict[type] = {'attempted': str(now.date().year), 'successful': None}
                return QueueItem.from_request(TreasuryRequest(type=type, year=str(now.date().year)), self.process_data, metadata={'type': type, 'year': str(now.date().year)})
            elif self.scrape_dict[type]['attempted'] == self.scrape_dict[type]['successful']:
                self.scrape_dict[type]['attempted'] = str(int(self.scrape_dict[type]['attempted']) - 1)
                return QueueItem.from_request(TreasuryRequest(type=type, year=self.scrape_dict[type]['attempted']), self.process_data, metadata={'type': type, 'year': self.scrape_dict[type]['attempted']})

    def process_data(self, data):
        if data.get_response().status_code != 200:
            return

        parsed_data = TreasuryRequest.parse_response(data.get_response().get_data())
        a = 1