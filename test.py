from datetime import datetime

from yfk import Quote, InsiderTransactions

# q = Quote('CL=F', period1=datetime(year=2017, month=5, day=3, hour=9, minute=30), period2=datetime(year=2017, month=5, day=3, hour=14, minute=30), interval='1m')
# for d in q.get_data()['data']:
# 	print d

# it = InsiderTransactions('FB')
# print it.get_data()


from DataGather import gather

gather()