from copy import deepcopy

from bs4 import BeautifulSoup, SoupStrainer

URL = 'http://etfdb.com/screener/#page={}'


class EtfDbSymbolRequest():
    def __init__(self, page):
        self.page = page

    def get_http_method(self):
        return 'GET'

    def get_url(self):
        url = deepcopy(URL)
        return url.format(self.page)

    @staticmethod
    def parse_response(response):
        data = []
        try:
            table_string = response[response.index('<table'):response.index('</table')+8]
        except ValueError:
            return data
        rows = BeautifulSoup(table_string, 'html.parser').find_all('tr')
        for row in rows:
            tds = row.find_all('td')
            if len(tds) == 0:
                continue
            document = {}
            for td in tds:
                try:
                    document[td.attrs['data-th']] = td.text.strip('\n')
                except KeyError:
                    document = {}
                    break
            if document:
                data.append(document)
        return data

if __name__ == '__main__':
    import requests
    all_data = []
    for page in range(1, 88):
        try:
            req = EtfDbSymbolRequest(page)
            a = requests.get(req.get_url())
            all_data += EtfDbSymbolRequest.parse_response(a.content)
        except:
            a = 1
    print all_data
