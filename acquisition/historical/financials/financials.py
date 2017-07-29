from bs4 import BeautifulSoup

from db import FinanceDB
from logger import Logger
from discord.webhook import DiscordWebhook
from acquisition.symbol.tickers import StockTickers
from utils.webdriver import Selenium

INCOME_STATEMENT_URL = 'https://finance.yahoo.com/quote/{}/financials'
BALANCE_SHEET_URL = 'https://finance.yahoo.com/quote/{}/balance-sheet'
CASH_FLOW_URL = 'https://finance.yahoo.com/quote/{}/cash-flow'

FINANCIAL_DOCUMENTS = ('income_statement', 'balance_sheet', 'cash_flow')
LOG_PERCENT = 10

class Financials():

    def __init__(self):
        self.task_name = 'Financials'
        self.finance_db = FinanceDB('financials')
        self.discord = DiscordWebhook()
        self.symbols = StockTickers().get_all()
        self.counter = 0
        self.income_statement = None
        self.balance_sheet = None
        self.cash_flow = None
        self.driver = None
        self._reset_counters()
        self.data = {}
        self.write_limit = 10
        self.last_benchmark = 0

    def _reset_counters(self):
        self.found = 0
        self.not_found = 0

    def _log(self, msg, level='info'):
        Logger.log(msg, level=level, threadname=self.task_name)

    def _log_process(self):
        if len(self.symbols) > 0:
            progress = (float(self.counter) / len(self.symbols)) * 100
            if progress > self.last_benchmark:
                self._log(str(round(float(progress), 2)) + '%')
                self.last_benchmark += LOG_PERCENT

    def get_financials(self, ticker):
        for url, var in ((INCOME_STATEMENT_URL, 'income_statement'), (BALANCE_SHEET_URL, 'balance_sheet'), (CASH_FLOW_URL, 'cash_flow')):
            try:
                self.driver.get(url.format(ticker))
            except Exception, e:
                continue
            setattr(self, var, self.get_financial_document())
        if all((self.income_statement, self.balance_sheet, self.cash_flow)):
            self.data[ticker] = {'income_statement': self.income_statement, 'balance_sheet': self.balance_sheet, 'cash_flow': self.cash_flow}

    def get_financial_document(self):
        quarterly_button = filter(lambda x: x.text == 'Quarterly', self.driver.find_elements_by_tag_name('span'))[0]
        quarterly_button.click()

        document = BeautifulSoup(self.driver.page_source, "html.parser")
        table = document.findChildren('table')[1]
        rows = table.findChildren('tr')
        data = {}
        dates = []
        row_titles = []
        for row in rows:
            row_data = map(lambda x: x.text, row.findChildren('td'))
            if not data:
                for i in range(1, len(row_data)):
                    dates.append(row_data[i])
                    data[row_data[i]] = {}
            else:
                row_title = row_data[0].replace('.', '')
                row_titles.append(row_title)
                for i in range(1, len(row_data)):
                    data[dates[i-1]][row_title] = row_data[i]
        return data

    def write_to_mongo(self):
        documents = []
        for symbol, financials in self.data.iteritems():
            for collection in FINANCIAL_DOCUMENTS:
                for date, data in financials[collection].iteritems():
                    data['period_ending'] = date
                    data['symbol'] = symbol
                    data['document'] = collection
                    documents.append(data)
        for document in documents:
            query = {}
            query['symbol'] = document['symbol']
            query['period_ending'] = document['period_ending']
            query['document'] = document['document']
            if len(list(self.finance_db.find(query))) == 0:
                try:
                    self.finance_db.insert_one(document)
                except Exception, e:
                    a = 1


    def quit_phantom_js(self):
        if self.driver and hasattr(self.driver, 'quit'):
            self.driver.quit()

    def next(self):
        self._log_process()
        if self.counter >= len(self.symbols):
            self.counter = 0
            self.symbols = StockTickers().get_all()
            raise StopIteration
        try:
            symbol = self.symbols[self.counter]
            if not self.driver:
                self.driver = Selenium().get_driver()
            self.get_financials(symbol)
            if len(self.data.keys()) >= 10 or self.counter + 1  >= len(self.symbols):
                self.write_to_mongo()
                self.data.clear()
            self.counter += 1
        except Exception, e:
            print e
            pass
        finally:
            self.quit_phantom_js()


if __name__ == "__main__":
    f = Financials()
    while 1:
        f.next()