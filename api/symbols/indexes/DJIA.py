from api.symbols.indexes.BaseIndex import BaseIndex

class DJIA(BaseIndex):

    def get_exchange(self):
        return ''

    def get_symbol(self):
        return 'DJIA'

    def get_country(self):
        return 'united-states'