from api.symbols.indexes.DJIA import DJIA
from api.symbols.indexes.DollarIndex import DollarIndex
from api.symbols.indexes.Nasdaq import Nasdaq
from api.symbols.indexes.SPX import SPX
from api.symbols.indexes.VIX import VIX

ALL_INDEXES = (
    DJIA(),
    DollarIndex(),
    Nasdaq(),
    SPX(),
    VIX()
)