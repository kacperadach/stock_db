from datetime import datetime

from yfk import Quote, InsiderTransactions

# q = Quote('CL=F', period1=datetime(year=2017, month=5, day=3, hour=9, minute=30), period2=datetime(year=2017, month=5, day=3, hour=14, minute=30), interval='1m')
# for d in q.get_data()['data']:
# 	print d

# it = InsiderTransactions('FB')
# print it.get_data()


from acquisition import gather

#gather()

# from yfk.options import Options
#
# o = Options('aapl', auto_query=True)
# data = o.get_data()
# print data
from datetime import date
from db import ScheduleDB

s = ScheduleDB(host='localhost', user='root', pwd='mobydick420', db='test')
# s.create_options_task('aapl', date(year=2017, month=5, day=12))
# s.commit_options_tasks()
s.complete_options_task('aapl', date(year=2017, month=5, day=12))