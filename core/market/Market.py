from datetime import time

TRADING_DAY_OPEN = time(hour=9, minute=30)
TRADING_DAY_CLOSE = time(hour=16)

def is_market_open(current_datetime):
    if current_datetime.weekday() > 4:
        return False
    return TRADING_DAY_OPEN <= current_datetime.time() <= TRADING_DAY_CLOSE

