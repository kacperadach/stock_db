from copy import deepcopy

from bs4 import BeautifulSoup, SoupStrainer

URL = 'https://www.treasury.gov/resource-center/data-chart-center/interest-rates/Pages/TextView.aspx?data={}Year&year={}'
TYPES = ('yield', 'billrates', 'longtermrate', 'realyield', 'reallongtermrate')

# https://www.cnbc.com/bonds/

class TreasuryRequestException(Exception):
    pass

class TreasuryRequest():
    def __init__(self, type, year):
        if type not in TYPES:
            raise TreasuryRequestException('Invalid request type: {}'.format(type))
        self.type = type
        self.year = year

    def get_http_method(self):
        return 'GET'

    def get_headers(self):
        return {}

    def get_body(self):
        return {}

    def get_url(self):
        url = deepcopy(URL)
        url = url.format(self.type, self.year)
        return url

    @staticmethod
    def parse_response(response):
        data = []
        table = SoupStrainer('table', {'class': 't-chart'})
        bs = BeautifulSoup(response, 'html.parser', parse_only=table)
        all_headers = []
        rows = bs.find_all('tr')
        for row in rows:
            if len(row.find_all('th')) > 0:
                headers = []
                for header in row.find_all('th'):
                    if 'colspan' in header.attrs.keys():
                        for _ in range(int(header.attrs['colspan'])):
                            headers.append(header.text)
                    else:
                        headers.append(header.text)
                all_headers.append(headers)
            else:
                for h in zip(*headers):
                    pass
                # row_data = map(lambda x: x.text, row.find_all('td'))
                # data.append(dict(zip(all_headers, row_data)))
        return data

if __name__ == '__main__':
    import requests
    tr = TreasuryRequest('billrates', '2018')
    response = requests.get(tr.get_url())
    TreasuryRequest.parse_response(response.text)
