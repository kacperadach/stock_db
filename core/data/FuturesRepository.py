from acquisition.symbol.futures import FUTURES

class FuturesRepository():

    @staticmethod
    def get_all_futures():
        return [symbol for _, symbols in FUTURES.items() for symbol in symbols]
