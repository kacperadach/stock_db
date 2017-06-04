from acquisition.daily.commodities import get_commodities_data

import datetime
a = get_commodities_data('CL=F', trading_date=datetime.datetime(year=2017, month=6, day=1).date())
print a