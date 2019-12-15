from copy import deepcopy

from bs4 import SoupStrainer, BeautifulSoup

from request.base.ResponseWrapper import ResponseWrapper

DOCUMENT_TYPES = (
    'income-statement',
    'cash-flow',
    'balance-sheet'
)

PERIODS = (
    'annual',
    'quarterly'
)

HEADERS = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9,pl;q=0.8',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36'
}

# BASE = 'https://www.barchart.com/stocks/quotes/KL/income-statement/annual'
BASE = 'https://www.barchart.com/stocks/quotes/{}/{}/{}?reportPage={}'

class BarchartFinancialsRequestException(Exception):
    pass

class BarchartFinancialsRequest:

    def __init__(self, symbol, document_type, period, page=1):
        self.symbol = symbol
        self.document_type = document_type
        self.period = period
        self.page = page
        if document_type not in DOCUMENT_TYPES:
            raise BarchartFinancialsRequestException('invalid document type')
        if period not in PERIODS:
            raise BarchartFinancialsRequestException('invalid period')

    def get_http_method(self):
        return 'GET'

    def get_headers(self):
        return deepcopy(HEADERS)

    def get_url(self):
        return deepcopy(BASE).format(self.symbol, self.document_type, self.period, self.page)

    @staticmethod
    def get_next_page(response):
        links = BeautifulSoup(response, 'html.parser', parse_only=SoupStrainer('div', {'class': 'bc-financial-report'})).find_all('a')

        if links:
            for l in links:
                if l.text.lower() == 'next':
                    return l.attrs['href'].split('=')[1]


    @staticmethod
    def parse_response(response):
        rows = BeautifulSoup(response, 'html.parser').find('tbody').find_all('tr')
        parsed_data = {}
        # parsed_rows = []
        header_stack = []
        dates = None
        for row in rows:
            parsed = list(map(lambda x: x.text.strip(), row.find_all('td')))
            # parsed_rows.append(parsed)
            if dates is None:
                dates = list(filter(lambda x: bool(x), parsed))
                for d in dates:
                    parsed_data[d] = {}
            elif len(parsed) == 2 and parsed[1] == '':
                header_stack.append(parsed[0])
                for key in parsed_data.keys():
                    temp = parsed_data[key]
                    for header in header_stack:
                        if header not in temp.keys():
                            temp[header] = {}
                        temp = temp[header]

            elif not any(parsed):
                try:
                    header_stack.pop()
                except IndexError:
                    pass
            else:
                key = parsed[0]
                values = parsed[1:]
                for index, date in enumerate(dates):
                    temp = parsed_data[date]
                    for header in header_stack:
                        temp = temp[header]
                    val = values[index].replace(',', '').replace('$', '')
                    if val == 'N/A':
                        val = ''
                    else:
                        val = float(val)
                    temp[key] = val
        return parsed_data



if __name__ == '__main__':
    import requests
    for period in PERIODS:
        for document_type in DOCUMENT_TYPES:
            r = BarchartFinancialsRequest('AAPL', document_type, period)
            response = requests.get(r.get_url(), headers=r.get_headers())
            rw = ResponseWrapper(response)
            data = BarchartFinancialsRequest.parse_response(rw.get_data())
            a = 1
