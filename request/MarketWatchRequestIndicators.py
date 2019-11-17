from copy import deepcopy

INDICATOR_JSON = {
    'Parameters': [],
    'Kind': '',
    'SeriesId': ''
}

INDICATOR_KEY_DICT = {
    'rsi': 'RelativeStrengthIndex',
    'macd': 'MovingAverageConvergenceDivergence',
    'macd-hist': 'MovingAverageConvergenceDivergence',
    'macd-signal': 'MovingAverageConvergenceDivergence'
}

COMPLEX_INDICATORS = ('macd', 'macd-hist', 'macd-signal')

class MarketWatchRequestIndicatorsException(Exception):
    pass

class MarketWatchRequestIndicators():

    def __init__(self, use_default=False):
        self.series_id = 2
        self.indicators = []
        self.use_default = use_default
        if use_default:
            self.volume()
            self.simple_moving_average(periods=[50, 100, 200])
            self.exponential_moving_average(periods=[50, 100, 200])
            # self.bollinger_bands()
            # self.momentum()
            # self.money_flow_index()
            # self.on_balance_volume()
            # self.relative_strength_index()
            # self.macd()
            # self.split_events()
            # self.dividends()
            # self.earnings()
            # self.price_to_earnings_ratio()

    def __repr__(self):
        return '<DEFAULT>' if self.use_default else '<NON-DEFAULT>'

    def get_indicator_parameter(self, original_key):
        key = original_key.lower()
        if key not in INDICATOR_KEY_DICT.keys():
            return original_key
        for indicator in self.indicators:
            if indicator['Kind'] == INDICATOR_KEY_DICT.get(key):
                if key not in COMPLEX_INDICATORS:
                    return original_key + '(' + str(indicator['Parameters'][0]['Value']) + ')'
                elif key == 'macd':
                    period = filter(lambda x: x['Name'] == 'EMA1', indicator['Parameters'])[0]['Value']
                    return original_key + '(' + str(period) + ')'
                elif key == 'macd-hist':
                    period = filter(lambda x: x['Name'] == 'EMA2', indicator['Parameters'])[0]['Value']
                    return original_key + '(' + str(period) + ')'
                elif key == 'macd-signal':
                    period = filter(lambda x: x['Name'] == 'SignalLine', indicator['Parameters'])[0]['Value']
                    return original_key + '(' + str(period) + ')'

    def get_indicators(self):
        return self.indicators

    def get_series_id(self):
        series_id = 'i' + str(self.series_id)
        self.series_id += 1
        return series_id

    def simple_moving_average(self, periods=(50,)):
        indicator_json = deepcopy(INDICATOR_JSON)
        for period in periods:
            indicator_json['Parameters'].append({'Name': 'Period', 'Value': period})
        indicator_json['Kind'] = 'SimpleMovingAverage'
        indicator_json['SeriesId'] = self.get_series_id()
        self.indicators.append(indicator_json)

    def exponential_moving_average(self, periods=(50,)):
        indicator_json = deepcopy(INDICATOR_JSON)
        for period in periods:
            indicator_json['Parameters'].append({'Name': 'Period', 'Value': period})
        indicator_json['Kind'] = 'ExponentialMovingAverage'
        indicator_json['SeriesId'] = self.get_series_id()
        self.indicators.append(indicator_json)

    def relative_strength_index(self, period=14):
        indicator_json = deepcopy(INDICATOR_JSON)
        indicator_json['Parameters'].append({'Name': 'Period', 'Value': period})
        indicator_json['Kind'] = 'RelativeStrengthIndex'
        indicator_json['SeriesId'] = self.get_series_id()
        self.indicators.append(indicator_json)

    def price_to_earnings_ratio(self):
        indicator_json = deepcopy(INDICATOR_JSON)
        indicator_json['Kind'] = 'PriceToEarningsRatio'
        indicator_json['SeriesId'] = self.get_series_id()
        self.indicators.append(indicator_json)

    def macd(self, period1=12, period2=26, signal_period=9):
        indicator_json = deepcopy(INDICATOR_JSON)
        indicator_json['Parameters'].append({'Name': 'EMA1', 'Value': period1})
        indicator_json['Parameters'].append({'Name': 'EMA2', 'Value': period2})
        indicator_json['Parameters'].append({'Name': 'SignalLine', 'Value': signal_period})
        indicator_json['Kind'] = 'MovingAverageConvergenceDivergence'
        indicator_json['SeriesId'] = self.get_series_id()
        self.indicators.append(indicator_json)

    def bollinger_bands(self, period=20, multiplier=2):
        indicator_json = deepcopy(INDICATOR_JSON)
        indicator_json['Parameters'].append({'Name': 'Period', 'Value': period})
        indicator_json['Parameters'].append({'Name': 'Multiplier', 'Value': multiplier})
        indicator_json['Kind'] = 'BollingerBands'
        indicator_json['SeriesId'] = self.get_series_id()
        self.indicators.append(indicator_json)

    def momentum(self, period=12):
        indicator_json = deepcopy(INDICATOR_JSON)
        indicator_json['Parameters'].append({'Name': 'Period', 'Value': period})
        indicator_json['Kind'] = 'Momentum'
        indicator_json['SeriesId'] = self.get_series_id()
        self.indicators.append(indicator_json)

    def money_flow_index(self, period=14):
        indicator_json = deepcopy(INDICATOR_JSON)
        indicator_json['Parameters'].append({'Name': 'Period', 'Value': period})
        indicator_json['Kind'] = 'MoneyFlowIndex'
        indicator_json['SeriesId'] = self.get_series_id()
        self.indicators.append(indicator_json)

    def on_balance_volume(self):
        indicator_json = deepcopy(INDICATOR_JSON)
        indicator_json['Kind'] = 'OnBalanceVolume'
        indicator_json['SeriesId'] = self.get_series_id()
        self.indicators.append(indicator_json)

    def dividends(self):
        indicator_json = deepcopy(INDICATOR_JSON)
        indicator_json['Kind'] = 'DividendEvents'
        indicator_json['SeriesId'] = self.get_series_id()
        self.indicators.append(indicator_json)

    def earnings(self):
        indicator_json = deepcopy(INDICATOR_JSON)
        indicator_json['Parameters'].append({'Name': 'YearOverYear'})
        indicator_json['Kind'] = 'EarningsEvents'
        indicator_json['SeriesId'] = self.get_series_id()
        self.indicators.append(indicator_json)

    def split_events(self):
        indicator_json = deepcopy(INDICATOR_JSON)
        indicator_json['Kind'] = 'SplitEvents'
        indicator_json['SeriesId'] = self.get_series_id()
        self.indicators.append(indicator_json)

    def volume(self):
        indicator_json = deepcopy(INDICATOR_JSON)
        indicator_json['Kind'] = 'Volume'
        indicator_json['SeriesId'] = self.get_series_id()
        self.indicators.append(indicator_json)
