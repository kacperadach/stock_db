import MarketWatchScraper

SYMBOL_COLLECTION = 'market_watch_symbols'
DATA_COLLECTION = 'futures'
INSTRUMENT_TYPE = 'futures'

# FUTURE/US/IFUS/CCK8

class Futures(MarketWatchScraper):

    def __init__(self):
        super(Futures, self).__init__(INSTRUMENT_TYPE, SYMBOL_COLLECTION, DATA_COLLECTION)

    def get_market_watch_symbol(self, symbol):
        return