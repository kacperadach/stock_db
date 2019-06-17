from api.symbols.BaseSymbol import BaseSymbol

INSTRUMENT_TYPE = 'currencies'

class BaseCurrency(BaseSymbol):

    def get_instrument_type(self):
        return INSTRUMENT_TYPE