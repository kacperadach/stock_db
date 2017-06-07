import json
import logging

import requests

logging.getLogger("requests").setLevel(logging.WARNING)

OPTIONS_URL = "https://query1.finance.yahoo.com/v7/finance/options/{}?lang=en-US&region=US&corsDomain=finance.yahoo.com"

OPTIONS_DATE_URL = "https://query2.finance.yahoo.com/v7/finance/options/{}?lang=en-US&region=US&date={}&corsDomain=finance.yahoo.com"

class OptionsError(Exception):
    pass


class Options():

    def __init__(self, symbol, auto_query=False):
        self.options = {}
        self.response = {}
        self.symbol = symbol
        self.auto_query = auto_query
        self.url = OPTIONS_URL.format(self.symbol)

        self.response = self._make_request()
        if not self._is_valid_response():
            raise OptionsError('Options not found for symbol: {}'.format(symbol))
        self.expiration_dates = self.response['expirationDates']
        self._append_options(self.response['options'])
        if self.expiration_dates and self.auto_query:
            for expirationDate in self.expiration_dates:
                self._change_url(expirationDate)
                try:
                    self._append_options(self._make_request()['options'])
                except:
                    pass
            self.response['options'] = self.options

    def _make_request(self):
        req = requests.get(self.url)
        try:
            data = json.loads(req.text)['optionChain']['result']
        except ValueError:
            data = []
        if data and len(data) != 0:
            return data[0]
        return data

    def _change_url(self, date):
        self.url = OPTIONS_DATE_URL.format(self.symbol, date)

    def _append_options(self, options):
        if options:
            date = options[0]['expirationDate']
            self.options[date] = {'calls': options[0]['calls'], 'puts': options[0]['puts']}

    def _is_valid_response(self):
        return bool(self.response)

    def get_data(self):
        return self.response
