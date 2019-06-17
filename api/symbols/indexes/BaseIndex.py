from api.symbols.BaseSymbol import BaseSymbol

INSTRUMENT_TYPE = 'indexes'

class BaseIndex(BaseSymbol):

    def get_instrument_type(self):
        return INSTRUMENT_TYPE