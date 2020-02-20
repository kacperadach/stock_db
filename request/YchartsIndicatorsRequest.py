from copy import deepcopy
from bs4 import BeautifulSoup, SoupStrainer
from datetime import datetime, timedelta
import pytz
from urllib.parse import quote_plus

from request.BaseRequest import BaseRequest
from request.base.ResponseWrapper import ResponseWrapper

# INDICATOR_BASE_URL = 'https://ycharts.com/indicators/cboe_equity_put_call_ratio'
INDICATOR_BASE_URL = 'https://ycharts.com{}'

ALL_INDICATORS_BASE_URL = 'https://ycharts.com/indicators'

HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9,pl;q=0.8',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Host': 'ycharts.com',
    'Pragma': 'no-cache',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36',
}

DATA_BASE_URL = 'https://ycharts.com/charts/fund_data.json?securities=include:true,id:{},,&calcs=&correlations=&format=real&recessions=false&zoom=custom&startDate={}&endDate={}&chartView=&splitType=single&scaleType=linear&note=&title=&source=false&units=false&quoteLegend=true&partner=basic_850&quotes=&legendOnChart=true&securitylistSecurityId=&clientGroupLogoUrl=&displayTicker=false&ychartsLogo=True&useEstimates=false&maxPoints=1152'

ALL_REPORTS_URL = 'https://ycharts.com/indicators/reports'

# CATEGORY_INDICATORS_BASE = 'https://ycharts.com/indicators/categories/nonfinancial_business_assets_and_liabilities/indicators?region_type=countries&region_code=USA'
CATEGORY_INDICATORS_BASE = 'https://ycharts.com{}/indicators?region_type=countries&region_code=USA&p={}&s=name&d=asc'

IGNORED_CATEGORIES = ('Countries', 'States', 'Reports', 'Sources')

# DATA_COOKIES_STRING = 'd-a8e6=62d00c04-19d4-4bc5-8c93-cdf16b5f7fca; s-9da4=a8a848d7-a84d-44b7-bc9f-8a1680599c3e; wcsid=CEHTGA6ZMVLQ3duW1y8Lx0U6BHbHaW3o; hblid=Nm3D0bwrregQ9QU01y8Lx0UHoBa3Hr6Z; _okdetect=%7B%22token%22%3A%2215805732236520%22%2C%22proto%22%3A%22https%3A%22%2C%22host%22%3A%22ycharts.com%22%7D; olfsk=olfsk09806761488580995; _okbk=cd4%3Dtrue%2Cwa1%3Dfalse%2Cvi5%3D0%2Cvi4%3D1580573223891%2Cvi3%3Dactive%2Cvi2%3Dfalse%2Cvi1%3Dfalse%2Ccd8%3Dchat%2Ccd6%3D0%2Ccd5%3Daway%2Ccd3%3Dfalse%2Ccd2%3D0%2Ccd1%3D0%2C; _ok=1228-592-10-8601; page_view_ctr=3; ycsessionid=2rnahfnb3r01o7ks52vd9tmlg4rh8ggf; _oklv=1580573287553%2CCEHTGA6ZMVLQ3duW1y8Lx0U6BHbHaW3o'


class YchartsAllCategoriesRequest:

    def __init__(self):
        pass

    def get_body(self):
        return {}

    def get_http_method(self):
        return 'GET'

    def get_url(self):
        return deepcopy(ALL_INDICATORS_BASE_URL)

    def get_headers(self):
        return deepcopy(HEADERS)

    @staticmethod
    def parse_response(response):
        divs = BeautifulSoup(response, 'html.parser', parse_only=SoupStrainer('div', {'class': 'indicCat'}))

        categories = []
        for div in divs:
            category = div.find('h3').text
            if category in IGNORED_CATEGORIES:
                continue

            subcategories = div.find_all('a')
            for subcategory in subcategories:
                categories.append({'link': subcategory.attrs['href'], 'category': category})

        return categories


class YchartCategoryIndicatorsRequest:

    def __init__(self, category, page=1):
        self.category = category
        self.page = page

    def get_body(self):
        return {}

    def get_http_method(self):
        return 'GET'

    def get_url(self):
        return deepcopy(CATEGORY_INDICATORS_BASE).format(self.category, self.page)

    def get_headers(self):
        return deepcopy(HEADERS)

    @staticmethod
    def has_next_page(response):
        pagination = BeautifulSoup(response, 'html.parser', parse_only=SoupStrainer('ol', {'class': 'pagin'}))

        for button in pagination.find_all('a'):
            if button.text.lower() == 'next':
                link = button.attrs['href']
                return link[link.find('p') + 2]

    @staticmethod
    def parse_response(response):
        table = BeautifulSoup(response, 'html.parser', parse_only=SoupStrainer('table', {'class': 'sortTable'}))
        body = table.find('tbody')

        if body is None:
            return []

        indicators = []
        for row in body.find_all('tr'):
            link = row.find('a')
            indicators.append({
                'link': link.attrs['href'],
                'name': link.text
            })

        return indicators


class YchartIndicatorIdRequest:

    def __init__(self, indicator):
        self.indicator = indicator

    def get_body(self):
        return {}

    def get_http_method(self):
        return 'GET'

    def get_url(self):
        return deepcopy(INDICATOR_BASE_URL).format(self.indicator)

    def get_headers(self):
        return deepcopy(HEADERS)

    @staticmethod
    def parse_response(response):
        div = BeautifulSoup(response, 'html.parser', parse_only=SoupStrainer('div', {'class': 'watchlistInlineFrame'}))

        for d in div.find_all('div'):
            for attr in d.attrs.items():
                if attr[0] == 'security-id':
                    return attr[1]


class YchartIndicatorRequest:

    def __init__(self, id, start, end):
        self.id = id
        self.start = start
        self.end = end

    def get_body(self):
        return {}

    def get_http_method(self):
        return 'GET'

    def get_url(self):
        return deepcopy(DATA_BASE_URL).format(self.id, quote_plus(self.start.strftime('%m/%d/%Y')),
                                              quote_plus(self.end.strftime('%m/%d/%Y')))

    def get_headers(self):
        headers = deepcopy(HEADERS)
        # headers['Cookie'] = deepcopy(DATA_COOKIES_STRING)
        # headers['X - Requested - With'] = 'XMLHttpRequest'
        # headers['Referer'] = 'https://ycharts.com/indicators/cboe_index_calls/chart/'
        return headers

    @staticmethod
    def parse_response(response):
        if not isinstance(response, dict) or 'chart_data' not in response.keys() or len(response['chart_data']) == 0 or len(response['chart_data'][0]) == 0:
            return {}

        chart_data = response['chart_data'][0][0]

        if 'raw_data' not in chart_data.keys() or not chart_data['raw_data']:
            return {}

        data = {
            'name': chart_data['long_label'],
            'currency_code': chart_data['currency_code'],
            'frequency': chart_data['frequency'],
            'start': response['start_date'],
            'end': response['end_date']
        }

        parsed_data = []
        for d in chart_data['raw_data']:
            parsed_data.append(
                {
                    'trading_date': datetime.fromtimestamp(d[0] / 1000, tz=pytz.utc),
                    'value': d[1]
                })

        data['data'] = parsed_data
        return data


if __name__ == '__main__':
    import requests

    # request = YchartsAllCategoriesRequest()
    # data = ResponseWrapper(requests.get(request.get_url()))
    # categories = YchartsAllCategoriesRequest.parse_response(data.get_data())
    #
    # for cat in categories:
    #     req = YchartCategoryIndicatorsRequest(cat['link'])
    #     data = ResponseWrapper(requests.get(req.get_url()))
    #     indicators = YchartCategoryIndicatorsRequest.parse_response(data.get_data())
    #
    #     for ind in indicators:
    # req = YchartIndicatorIdRequest(ind['link'])
    req = YchartIndicatorIdRequest('/indicators/cboe_equity_put_call_ratio')
    data = ResponseWrapper(requests.get(req.get_url()))
    id = YchartIndicatorIdRequest.parse_response(data.get_data())

    all_data = []

    start = datetime.today()
    while 1:
        req = YchartIndicatorRequest(id, start - timedelta(days=365), start)
        data = ResponseWrapper(requests.get(req.get_url()))
        d = YchartIndicatorRequest.parse_response(data.get_data())
        if d:
            all_data.append(d)
            start = start - timedelta(days=365)
        else:
            break


# ScraperQueueManager: Error occurred in callback for metadata {'type': 'indicator_id', 'indicator': {'link': '/indicators/us_arts_entertainment_and_recreation_corporate_profits_after_tax_yearly', 'name': 'US Arts, Entertainment, and Recreation Corporate Profits After Tax'}, 'category': {'link': '/indicators/categories/components_of_gdp', 'category': 'National Accounts'}}:
# Traceback (most recent call last):
#   File "/root/stock_db/core/BaseScraper.py", line 53, in callback
#     self.request_callback(queue_item)
#   File "/root/stock_db/acquisition/scrapers/YchartsScraper.py", line 66, in request_callback
#     id = YchartIndicatorIdRequest.parse_response(queue_item.get_response().get_data())
#   File "/root/stock_db/request/YchartsIndicatorsRequest.py", line 141, in parse_response
#     div = BeautifulSoup(response, 'html.parser', parse_only=SoupStrainer('div', {'class': 'watchlistInlineFrame'}))
#   File "/usr/local/lib/python3.6/dist-packages/bs4/__init__.py", line 310, in __init__
#     markup, from_encoding, exclude_encodings=exclude_encodings)):
#   File "/usr/local/lib/python3.6/dist-packages/bs4/builder/_htmlparser.py", line 248, in prepare_markup
#     exclude_encodings=exclude_encodings)
#   File "/usr/local/lib/python3.6/dist-packages/bs4/dammit.py", line 381, in __init__
#     markup, override_encodings, is_html, exclude_encodings)
#   File "/usr/local/lib/python3.6/dist-packages/bs4/dammit.py", line 249, in __init__
#     self.markup, self.sniffed_encoding = self.strip_byte_order_mark(markup)
#   File "/usr/local/lib/python3.6/dist-packages/bs4/dammit.py", line 309, in strip_byte_order_mark
#     elif data[:3] == b'\xef\xbb\xbf':
# TypeError: unhashable type: 'slice'