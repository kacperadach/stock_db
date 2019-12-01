from datetime import timedelta

from acquisition.scrapers.MarketWatchLiveScraper import MarketWatchLiveScraper

from api.symbols.indexes.DJIA import DJIA
from api.symbols.indexes.DollarIndex import DollarIndex
from api.symbols.indexes.Nasdaq import Nasdaq
from api.symbols.indexes.SPX import SPX
from api.symbols.indexes.VIX import VIX

class IndexLiveScraper(MarketWatchLiveScraper):
    INDEX_SYMBOLS = (DJIA(), SPX(), Nasdaq(), VIX(), DollarIndex())

    def get_symbols(self):
        return self.db.find(self.MARKET_WATCH_SYMBOL_COLLECTION, {'symbol': {'$in': list(map(lambda x: x.get_symbol(), self.INDEX_SYMBOLS))}, 'instrument_type': 'indexes'}, {'symbol': 1, 'instrument_type': 1, 'exchange': 1, 'country': 1, 'country_code': 1})

    def get_time_delta(self):
        return timedelta(minutes=10)
