from api.symbols.indexes.BaseIndex import BaseIndex

class VIX(BaseIndex):

    def get_exchange(self):
        return 'CBSX'

    def get_country(self):
        return 'united-states'

    def get_symbol(self):
        return 'VIX'