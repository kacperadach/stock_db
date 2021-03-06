# from bs4 import BeautifulSoup
#
# from db.Finance import Finance_DB
# from discord.webhook import DiscordWebhook
# from acquisition.symbol.financial_symbols import Financial_Symbols
# from utils.webdriver import Selenium
#
# INCOME_STATEMENT_URL = 'https://finance.yahoo.com/quote/{}/financials'
# BALANCE_SHEET_URL = 'https://finance.yahoo.com/quote/{}/balance-sheet'
# CASH_FLOW_URL = 'https://finance.yahoo.com/quote/{}/cash-flow'
#
# FINANCIAL_DOCUMENTS = ('income_statement', 'balance_sheet', 'cash_flow')
# LOG_PERCENT = 10
#
# BATCH_SIZE = 10
#
# FINANCIALS_COLLECTION = 'financials'
#
# class Financials():
#
#     def __init__(self):
#         self.task_name = 'Financials'
#         self.finance_db = Finance_DB
#         self.discord = DiscordWebhook()
#         self.symbols = Financial_Symbols.get_all()
#         self.counter = 0
#         self.driver = None
#         self._reset_counters()
#         self.data = {}
#         self.last_benchmark = 0
#
#     def _reset_counters(self):
#         self.found = 0
#         self.not_found = 0
#
#     def _log(self, msg, level='info'):
#         pass
#
#     def _log_process(self):
#         if len(self.symbols) > 0:
#             progress = (float(self.counter) / len(self.symbols)) * 100
#             if progress > self.last_benchmark:
#                 self._log(str(round(float(progress), 2)) + '%')
#                 self.last_benchmark += LOG_PERCENT
#
#     def get_financials(self, ticker):
#         financials = {}
#         print ticker
#         for url, var in ((INCOME_STATEMENT_URL, 'income_statement'), (BALANCE_SHEET_URL, 'balance_sheet'), (CASH_FLOW_URL, 'cash_flow')):
#             try:
#                 financials[var] = self.get_financial_document(url.format(ticker))
#             except Exception, e:
#                 continue
#         if any(financials.itervalues()):
#             print 'found: {}'.format(ticker)
#             self.data[ticker] = financials
#
#     def get_financial_document(self, url):
#         self.driver.get(url)
#         quarterly_button = filter(lambda x: x.text == 'Quarterly', self.driver.find_elements_by_tag_name('span'))
#         if len(quarterly_button) == 0:
#             print 'quarterly button not found'
#             return {}
#
#         quarterly_button = quarterly_button[0]
#         quarterly_button.click()
#
#         document = BeautifulSoup(self.driver.page_source, "html.parser")
#         table = document.findChildren('table')[1]
#         rows = table.findChildren('tr')
#         data = {}
#         dates = []
#         row_titles = []
#         for row in rows:
#             row_data = map(lambda x: x.text, row.findChildren('td'))
#             if not data:
#                 for i in range(1, len(row_data)):
#                     dates.append(row_data[i])
#                     data[row_data[i]] = {}
#             else:
#                 row_title = row_data[0].replace('.', '')
#                 row_titles.append(row_title)
#                 for i in range(1, len(row_data)):
#                     data[dates[i-1]][row_title] = row_data[i]
#         return data
#
#     def write_to_mongo(self):
#         found_documents = []
#         for symbol, financials in self.data.items():
#             for collection in FINANCIAL_DOCUMENTS:
#                 for date, data in financials[collection].items():
#                     data['period_ending'] = date
#                     data['symbol'] = symbol
#                     data['document'] = collection
#                     found_documents.append(data)
#         new_documents = []
#         for document in found_documents:
#             query = {}
#             query['symbol'] = document['symbol']
#             query['period_ending'] = document['period_ending']
#             query['document'] = document['document']
#             if len(list(self.finance_db.find(query, FINANCIALS_COLLECTION).limit(1))) == 0:
#                 new_documents.append(document)
#
#         if new_documents:
#             self.finance_db.insert_many(new_documents, FINANCIALS_COLLECTION)
#
#     def quit_phantom_js(self):
#         if self.driver and hasattr(self.driver, 'quit'):
#             try:
#                 self.driver.quit()
#             except OSError:
#                 pass
#
#     def next(self):
#         self._log_process()
#         if self.counter >= len(self.symbols):
#             self.counter = 0
#             self.symbols = Financial_Symbols.get_all()
#             self.quit_phantom_js()
#             raise StopIteration
#         try:
#             symbol = self.symbols[self.counter]
#             if not self.driver:
#                 self.driver = Selenium().get_driver()
#             self.get_financials(symbol)
#             if len(self.data.keys()) >= BATCH_SIZE or self.counter + 1 >= len(self.symbols):
#                 self.write_to_mongo()
#                 self.data.clear()
#         except Exception, e:
#             self._log("Unexpected error occurred during execution: {}".format(str(e)))
#
#         finally:
#             self.counter += 1
#
#
#
# if __name__ == "__main__":
#     f = Financials()
#     while 1:
#         f.next()