import time
import datetime


from yfk.quote import Quote

period2 = int(time.mktime(datetime.datetime(2017, 6, 3, 0, 0, 0).timetuple()))
period1 = int(time.mktime(datetime.datetime(2017, 6, 2, 0, 0, 0).timetuple()))



q = Quote('CL=F', period1=period1, period2=period2, interval='1m')
a = 1
# q = q.get_data()['chart']['result'][0]['meta']['tradingPeriods']
#
# for key, val in q.items():
#     print key + '\n'
#     print 'start ' + datetime.datetime.fromtimestamp(val[0][0]['start']).strftime('%Y-%m-%d %H:%M:%S') + '\n'
#     print 'end ' + datetime.datetime.fromtimestamp(val[0][0]['end']).strftime('%Y-%m-%d %H:%M:%S') + '\n'


#print datetime.datetime.fromtimestamp(1496548800).strftime('%Y-%m-%d %H:%M:%S')