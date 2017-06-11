from acquisition.daily.options import get_all_options_data

import datetime
trading_date = datetime.date.today()
get_all_options_data(trading_date)
a = 1