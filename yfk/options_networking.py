from networking import Networking
from logger import Logger

OPTIONS_URL = "https://query1.finance.yahoo.com/v7/finance/options/{}?lang=en-US&region=US&corsDomain=finance.yahoo.com"
OPTIONS_DATE_URL = "https://query2.finance.yahoo.com/v7/finance/options/{}?lang=en-US&region=US&date={}&corsDomain=finance.yahoo.com"

BATCH_SIZE = 100

class OptionsError(Exception):
    pass

class Options():

    def __init__(self, symbols, batching=False, update_percent=10):
        self.task_name = 'OptionsAcquisition'
        self.options = {}
        self.symbols = symbols
        self.batching = batching
        self.last_benchmark = -1
        self.update_percent = update_percent
        if self.symbols and not self.batching:
            self.execute(symbols)

    def _log_process(self, batch):
        if batch / float(len(self.symbols)) * 100 > self.last_benchmark:
            self.last_benchmark += self.update_percent
            Logger.log(str(round((float(batch) / len(self.symbols)) * 100, 2)) + '%', threadname=self.task_name)

    def generate(self):
        for x in range(0, len(self.symbols), BATCH_SIZE):
            self._log_process(x)
            yield self.execute(self.symbols[x: x + BATCH_SIZE])
        self.last_benchmark = 0

    def _create_urls(self, symbols):
        self.urls = dict([(x, OPTIONS_URL.format(x)) for x in symbols])

    def execute(self, symbols):
        self._create_urls(symbols)
        self._make_requests()
        return self.data

    def _make_requests(self):
        n = Networking(threadname=self.task_name, log_progress=not self.batching)
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

    def get_data(self):
        return self.data

