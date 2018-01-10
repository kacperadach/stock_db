from datetime import datetime, date, timedelta, time
from pytz import timezone

from request.YahooFinanceStockRequest import YahooFinanceStockRequest
from request.base.RequestClient import RequestClient

INTERVALS = ('1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h', '1d')

class R():
    def __init__(self, url):
        self.url = url

def main():
    interval = '1m'
    r = RequestClient(use_tor=False)
    end_date = datetime.combine(datetime.now().date(), time()).replace(tzinfo=timezone('EST'))
    finished = False
    x = 1
    while not finished:
        request_url = YahooFinanceStockRequest('ttph', end_date - timedelta(days=x), end_date, interval=interval).get_url()
        print request_url
        response = r.get(R(request_url)).get_data()
        data = YahooFinanceStockRequest.parse_response(response)
        if 'dataGranularity' not in data['meta'].keys():
            finished = True
        elif data['meta']['dataGranularity'] != interval:
            finished = True
        else:
            x += 1
    print x



    # interval_time_ranges = {}
    # for interval in INTERVALS:
    #     interval_time_ranges[interval] = {"start": datetime.now(), "end": datetime.now()}
    # r = RequestClient(use_tor=False)
    # # end_date = datetime.combine(datetime.now().date(), time()).replace(tzinfo=timezone('EST'))
    # for interval in INTERVALS:
    #     end_date = datetime.combine(datetime.now().date(), time()).replace(tzinfo=timezone('EST'))
    #     interval_time_ranges[interval] = {"start": datetime.now(), "end": end_date}
    #     finished = False
    #     while finished is False:
    #         print interval + '- ' + 'end_date: ' + str(end_date)
    #         request_url = YahooFinanceStockRequest('aapl', end_date - timedelta(days=3), end_date, interval='1m').get_url()
    #         response = r.get(R(request_url)).get_data()
    #         data = YahooFinanceStockRequest.parse_response(response)
    #         if data['meta']['dataGranularity'] != interval or not data['data']:
    #             interval_time_ranges[interval] = {"start": end_date, "end": interval_time_ranges[interval]['end']}
    #             finished = True
    #         elif data['data']:
    #             end_date = end_date - timedelta(days=4)
    # print interval_time_ranges
    # a = 1


main()