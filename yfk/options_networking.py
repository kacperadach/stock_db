from networking import Networking

OPTIONS_URL = "https://query1.finance.yahoo.com/v7/finance/options/{}?lang=en-US&region=US&corsDomain=finance.yahoo.com"

OPTIONS_DATE_URL = "https://query2.finance.yahoo.com/v7/finance/options/{}?lang=en-US&region=US&date={}&corsDomain=finance.yahoo.com"

class OptionsError(Exception):
    pass

class Options():

    def __init__(self, symbols):
        self.options = {}
        self.response = {}
        self.symbols = symbols
        self.urls = dict([(x, OPTIONS_URL.format(x)) for x in symbols])
        self.response = self._make_requests()

    def _make_requests(self):
        n = Networking(max_threads=150)
        data = n.execute(self.urls)
        data = dict(map(lambda (k,v): (k, v['optionChain']['result'][0] if 'optionChain' in v.keys() and 'result' in v['optionChain'].keys() and len(v['optionChain']['result']) > 0 else {}), data.iteritems()))

        options_urls = {}
        for symbol, d in data.iteritems():
            try:
                expiration_dates = d['expirationDates']
            except KeyError:
                continue
            for exp_date in expiration_dates:
                options_urls[symbol + '-' + str(exp_date)] =  OPTIONS_DATE_URL.format(symbol, exp_date)

        options_data = n.execute(options_urls)
        options_data = dict(map(lambda (k,v): (k, v['optionChain']['result'][0] if 'optionChain' in v.keys() and 'result' in v['optionChain'].keys() and len(v['optionChain']['result']) > 0 else {}), options_data.iteritems()))

        for key, d in options_data.iteritems():
            symbol, exp_date = key.split('-')
            dat = data[symbol]
            if isinstance(dat['options'], list) and len(dat['options']) > 0:
                dat['options'] = {dat['options'][0]['expirationDate']: {'puts': dat['options'][0]['puts'], 'calls': dat['options'][0]['calls']}}
            elif isinstance(dat['options'], list) and len(dat['options']) == 0:
                dat['options'] = {}
            else:
                dat['options'][str(exp_date)] = {'puts': d['options'][0]['puts'], 'calls': d['options'][0]['calls']}
        self.data = data

    def _append_options(self, options):
        if options:
            date = options[0]['expirationDate']
            self.options[date] = {'calls': options[0]['calls'], 'puts': options[0]['puts']}

    def get_data(self):
        return self.data


from acquisition.symbol.tickers import StockTickers

tickers = StockTickers().get_all()
from datetime import datetime

start = datetime.now()
o = Options(tickers)
data = o.get_data()
print(datetime.now() - start)
a = 1