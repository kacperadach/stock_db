from yfk.fintel import FintelAcquisition

from acquisition.symbol.financial_symbols import Financial_Symbols
FintelAcquisition().execute_ownership(Financial_Symbols.get_all())
a = 1