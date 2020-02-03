from copy import deepcopy
from datetime import timedelta, datetime
import pytz

from core.BaseScraper import BaseScraper
from core.QueueItem import QueueItem
from core.data.YchartsRepository import YchartsRepository
from request.YchartsIndicatorsRequest import YchartsAllCategoriesRequest, YchartCategoryIndicatorsRequest, \
    YchartIndicatorIdRequest, YchartIndicatorRequest

ALL_CATEGORIES = 'all_categories'
CATEGORY_TYPE = 'category_type'
INDICATOR_ID = 'indicator_id'
INDICATOR_DATA = 'indicator_data'

class YchartsScraper(BaseScraper):

    def __init__(self):
        super(YchartsScraper, self).__init__()
        self.ycharts_repository = YchartsRepository()

    def get_symbols(self):
        return [{'type': ALL_CATEGORIES}]

    def get_queue_item(self, symbol):
        if symbol['type'] == ALL_CATEGORIES:
            return QueueItem.from_request(YchartsAllCategoriesRequest(), __name__, symbol)
        elif symbol['type'] == CATEGORY_TYPE:
            return QueueItem.from_request(YchartCategoryIndicatorsRequest(symbol['category']['link'], symbol['page']), __name__, symbol)
        elif symbol['type'] == INDICATOR_ID:
            return QueueItem.from_request(YchartIndicatorIdRequest(symbol['indicator']['link']), __name__, symbol)
        elif symbol['type'] == INDICATOR_DATA:
            return QueueItem.from_request(YchartIndicatorRequest(symbol['id'], symbol['start'], symbol['end']), __name__, symbol)

    def get_time_delta(self):
        return timedelta(days=2)

    def process_data(self, queue_item):
        if queue_item.get_metadata()['type'] != INDICATOR_DATA:
            return

        data = YchartIndicatorRequest.parse_response(queue_item.get_response().get_data())

        if data:
            self.ycharts_repository.insert(data, queue_item.get_metadata())

    def requests_per_second(self):
        return 1

    def request_callback(self, queue_item):
        request_type = queue_item.get_metadata()['type']
        if request_type == ALL_CATEGORIES:
            categories = YchartsAllCategoriesRequest.parse_response(queue_item.get_response().get_data())
            for category in categories:
                self.additional_symbols.append({'type': CATEGORY_TYPE, 'category': category, 'page': 1})
        elif request_type == CATEGORY_TYPE:
            next_page = YchartCategoryIndicatorsRequest.has_next_page(queue_item.get_response().get_data())
            if next_page is not None:
                meta = deepcopy(queue_item.get_metadata())
                meta['page'] = next_page
                self.additional_symbols.append(meta)
            indicators = YchartCategoryIndicatorsRequest.parse_response(queue_item.get_response().get_data())
            for indicator in indicators:
                self.additional_symbols.append({'type': INDICATOR_ID, 'indicator': indicator, 'category': queue_item.get_metadata()['category']})
        elif request_type == INDICATOR_ID:
            id = YchartIndicatorIdRequest.parse_response(queue_item.get_response().get_data())
            meta = deepcopy(queue_item.get_metadata())
            meta['id'] = id
            meta['type'] = INDICATOR_DATA
            end = datetime.today() + timedelta(days=1)
            start = end - timedelta(days=365 * 3)
            meta['end'] = end
            meta['start'] = start
            self.additional_symbols.append(meta)
        elif request_type == INDICATOR_DATA:
            data = YchartIndicatorRequest.parse_response(queue_item.get_response().get_data())
            # need to query DB here for oldest date
            if 'data' in data.keys() and data['data']:
                start = queue_item.get_metadata()['start'].replace(tzinfo=pytz.utc)
                end = queue_item.get_metadata()['end'].replace(tzinfo=pytz.utc)
                # if the values are not in the queried range, stop
                if any(map(lambda x: start <= x['trading_date'] <= end, data['data'])):
                    meta = deepcopy(queue_item.get_metadata())
                    last_start = meta['start']
                    new_start = last_start - timedelta(days=365 * 3)
                    meta['start'] = new_start
                    meta['end'] = last_start
                    self.additional_symbols.append(meta)

