from acquisition.scrapers.MarketWatchLiveScraper import MarketWatchLiveScraper


class CurrencyLiveScraper(MarketWatchLiveScraper):
    CURRENCY_SYMBOLS = ()

    def get_symbols(self):
        return self.db.find(self.MARKET_WATCH_SYMBOL_COLLECTION, {'symbol': {'$in': map(lambda x: x.get_symbol(), self.CURRENCY_SYMBOLS)}, 'instrument_type': 'indexes'},
                            {'symbol': 1, 'instrument_type': 1, 'exchange': 1, 'country': 1, 'country_code': 1})

