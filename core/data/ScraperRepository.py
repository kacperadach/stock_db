from core.StockDbBase import StockDbBase
from db.Finance import FinanceDB

COLLECTION_NAME = 'scraper_stats'

ALL_FIELDS = {
    'datetime_utc': 1,
    'request_queue_size': 1,
    'output_queue_size': 1,
    'requests': 1,
    'successful_requests': 1,
    'failed_requests': 1,
    'requests_per_second': 1
}

class ScraperRepository(StockDbBase):

    def __init__(self):
        super(ScraperRepository, self).__init__()
        self.db = FinanceDB()

    def save_request_interval(self, data):
        self.db.insert_one(COLLECTION_NAME, data)

    def get_recent(self):
        data = list(self.db.find(COLLECTION_NAME, {}, ALL_FIELDS).limit(200))
        new_data = []
        for d in data:
            d['datetime_utc'] = d['datetime_utc'].strftime('%Y-%m-%d %H:%M:%S')
            new_data.append(d)
        return new_data

Scraper_Repository = ScraperRepository()

if __name__ == '__main__':
    Scraper_Repository.get_recent()
