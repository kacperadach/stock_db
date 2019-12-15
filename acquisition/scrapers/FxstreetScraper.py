from datetime import datetime, timedelta

from core.BaseScraper import BaseScraper
from core.QueueItem import QueueItem
from core.data.FxstreetRepository import FxstreetRepository
from request.FxstreetRequest import FxstreetRequest

class FxstreetScraper(BaseScraper):

    def __init__(self):
        super(FxstreetScraper, self).__init__()
        self.fxstreet_repository = FxstreetRepository()

    def get_symbols(self):
        now = datetime.now()

        most_recent_date = self.fxstreet_repository.get_most_recent_event_date()
        start = datetime(year=1985, month=1, day=1)
        if most_recent_date is not None:
            if most_recent_date > now - timedelta(days=365):
                start = now - timedelta(days=365)
            else:
                start = most_recent_date

        while start < now + timedelta(days=180):
            end = start + timedelta(days=30)
            yield {
                'start': datetime(year=start.year, month=start.month, day=start.day),
                'end': datetime(year=end.year, month=end.month, day=end.day, hour=23, minute=59, second=59)
            }
            start = end

    def get_queue_item(self, symbol):
        request = FxstreetRequest(start=symbol['start'], end=symbol['end'])
        return QueueItem(
            url=request.get_url(),
            headers=request.get_headers(),
            http_method=request.get_http_method(),
            metadata=symbol
        )

    def get_time_delta(self):
        return timedelta(hours=12)

    def process_data(self, queue_item):
        if not queue_item.get_response().is_successful():
            return

        data = FxstreetRequest.parse_response(queue_item.get_response().get_data())
        if data:
            self.fxstreet_repository.insert(data)
