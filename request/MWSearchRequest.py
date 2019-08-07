from copy import deepcopy

BASE_URL = "https://services.dowjones.com/autocomplete/data?q={}&need=marketwatch-topic%2Cmarketwatch-search-link%2Csymbol&excludeExs=xmstar&featureClass=P&style=full&maxRows=12&name_startsWith={}&entitlementToken=cecc4267a0194af89ca343805a3e57af"

"""
HEADERS:

Accept: application/json
Origin: https://www.marketwatch.com
Referer: https://www.marketwatch.com/investing/stock/{ symbol }
User-Agent: Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36
"""

TYPE_TO_INSTRUMENT = {
    'Stock': 'stocks',
    'Currency': 'currency',
    'Bond': 'bonds',
    'Future': 'futures',
    'Rate': 'rates',
    'Fund': 'funds',
    'Benchmark': 'benchmarks',
    'Index': 'indexes',
    'CryptoCurrency': 'crypto-currencies',
    'Warrant': 'warrants',
    'AmericanDepositoryReceiptStock': 'american-depository-receipt-stocks',
    'ExchangeTradedNote': 'exchange-traded-notes',
    'ExchangeTradedFund': 'exchange-traded-funds',
    'RealEstateInvestmentTrust': 'real-estate-investment-trusts'
}

class MWSearchRequest():

    def __init__(self, query):
        self.query = query

    def get_url(self):
        url = deepcopy(BASE_URL)
        return url.format(self.query, self.query)

    def get_http_method(self):
        return 'GET'

    @staticmethod
    def parse_response(response):
        if 'symbols' not in response.iterkeys():
            print response
            return []

        symbols = []
        for symbol in response['symbols']:
            data = {
                'exchange': symbol['exchangeIsoCode'],
                'symbol': symbol['ticker'],
                'long_name': symbol['company'],
                'country': symbol['country'],
                'country_code': symbol['country'],
                'instrument_type': TYPE_TO_INSTRUMENT[symbol['type']],
                'from_search': True
            }
            symbols.append(data)

        return symbols
