from selenium import webdriver
from bs4 import BeautifulSoup

from discord.webhook import DiscordWebhook

ETF_URL = 'https://finance.yahoo.com/etfs?offset={}&count=100'

body = {
    "offset": 0,
    "size": "100",
    "sortType":"DESC",
    "sortField":"percentchange",
    "quoteType":"ETF",
    "query": {
        "operator":"and",
        "operands": [{
            "operator":"or",
            "operands": [{
                "operator":"EQ",
                "operands": ["region","us"]}]
        }]},
    "userId":"",
    "userIdType":"guid"
}


class ETF():

    def __init__(self):
        pass

    def get_all_etfs(self):
        max_etfs = None
        i = 0
        self.driver = webdriver.PhantomJS("C:/Users/Kacper/AppData/Roaming/npm/node_modules/phantomjs-prebuilt/lib/phantom/bin/phantomjs")

        self.driver.get(ETF_URL.format(i))
        page_source = self.driver.page_source
        etfs = BeautifulSoup(page_source, "html.parser")
        if max_etfs is None:
            for div in etfs.find_all('div'):
                if 'class' in div.attrs.keys():
                    if "Fw(b)" in div.attrs['class'] and "Fz(36px)" in div.attrs['class']:
                        max_etfs = int(div.text)

        if max_etfs is None:
            DiscordWebhook().send_message('Could not find max ETFs when scraping')
        else:
            self.etfs = []
            while i < max_etfs:
                self.driver.get(ETF_URL.format(i))
                page_source = self.driver.page_source
                etfs = BeautifulSoup(page_source, "html.parser")
                screener_table = etfs.findChildren('table')[1]
                for row in screener_table.findChildren('tr')[1:]:
                    td = filter(bool, map(lambda x: x.text, row.findChildren('td')))
                    self.etfs.append(td[0])
                i += 100

a = ETF()
a.get_all_etfs()
print a.etfs