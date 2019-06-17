from api.symbols.indexes.BaseIndex import BaseIndex

class DollarIndex(BaseIndex):
    def get_exchange(self):
        return 'IFUS'

    def get_symbol(self):
        return 'DXY'

    def get_country(self):
        return 'united-states'

