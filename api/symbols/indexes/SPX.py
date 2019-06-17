from api.symbols.indexes.BaseIndex import BaseIndex

class SPX(BaseIndex):
    def get_symbol(self):
        return 'SPX'

    def get_exchange(self):
        return ''

    def get_country(self):
        return 'united-states'