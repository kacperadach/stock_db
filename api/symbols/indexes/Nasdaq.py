from api.symbols.indexes.BaseIndex import BaseIndex

class Nasdaq(BaseIndex):
    def get_symbol(self):
        return 'NDX'

    def get_country(self):
        return 'united-states'

    def get_exchange(self):
        return 'XNAS'
