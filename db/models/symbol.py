from datetime import datetime

from sqlalchemy import *

from db.models import Base

class StockTicker(Base):
    __tablename__ = 'optiontask'

    symbol = Column(String(30), primary_key=True)
    trading_date = Column(Date, primary_key=True)
    created = Column(DateTime, default=datetime.now)
    completed = Column(Boolean, default=False)
