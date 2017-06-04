from datetime import datetime

from sqlalchemy import *

from db.models import Base

class OptionTask(Base):
    __tablename__ = 'optiontask'

    symbol = Column(String(30), primary_key=True)
    trading_date = Column(Date, primary_key=True)
    created = Column(DateTime, default=datetime.now)
    completed = Column(Boolean, default=False)

class TickerTask(Base):
    __tablename__ = 'tickertask'

    trading_date = Column(Date, primary_key=True)
    created = Column(DateTime, default=datetime.now)
    completed = Column(Boolean, default=False)

class InsiderTask(Base):
    __tablename__ = 'insidertask'

    symbol = Column(String(30), primary_key=True)
    trading_date = Column(Date, primary_key=True)
    created = Column(DateTime, default=datetime.now)
    completed = Column(Boolean, default=False)

class CommodityTask(Base):
    __tablename__ = 'commoditytask'

    symbol = Column(String(30), primary_key=True)
    trading_date = Column(Date, primary_key=True)
    created = Column(DateTime, default=datetime.now)
    completed = Column(Boolean, default=False)