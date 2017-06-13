from tornado import ioloop, httpclient

from yfk.options import Options, OptionsError
from acquisition.symbol.tickers import StockTickers

i = []

def handle_request(response):
    print(response.code)
    if response.code == 599:
        i.append(response.effective_url)

http_client = httpclient.AsyncHTTPClient()
for symbol in StockTickers().get_all():
    http_client.fetch(Options(symbol).url, handle_request, method='HEAD')
ioloop.IOLoop.instance().start()

a = 1
# # a = 1
# # import logging
# # logging.basicConfig()
# # logging.getLogger().setLevel(logging.DEBUG)
# # requests_log = logging.getLogger("requests.packages.urllib3")
# # requests_log.setLevel(logging.DEBUG)
# # requests_log.propagate = True
#
# import requests
# proxies = {
#     'http': 'socks5://localhost:9050',
#     'https': 'socks5://localhost:9050'
# }
# url = 'http://httpbin.org/ip'
# # #
# # # # print(requests.get(url).text)
# # #
# from stem import Signal
# from stem.control import Controller
# #
# # # # signal TOR for a new connection
# #
#
# controller = Controller.from_port(port = 9051)
# controller.authenticate(password="thickblunt420")
# ips = []
# for i in range(0, 100):
#     print i
#     if controller.is_newnym_available():
#         print 'Switchup'
#         controller.signal(Signal.NEWNYM)
#     try:
#         ip = requests.get(url, proxies=proxies).text
#         if ip not in ips:
#             ips.append(ip)
#     except:
#         print "Error"
# #
# #
# #
# #     controller.close()
# #
# # from requests.exceptions import ConnectionError
# #
# #
# #
# # print len(ips)
# # print ips
# #
# # # print(requests.get(url, proxies=proxies).text)
# # # renew_connection()
# # # print(requests.get(url, proxies=proxies).text)
# # # renew_connection()
# # # print(requests.get(url, proxies=proxies).text)
# #
# #
# # # ControlPort 9051
# # # HashedControlPassword 16:F5FFC4531E7B8B1A609C2C362439DA5113432D0A7A7645AEC53A7B2B36
# #
#
#
# import socket
# s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# s.connect(("www.google.com", 80))
# a = 1
#
# from acquisition.symbol.tickers import StockTickers
#
# f = open('url.txt', 'w')
# for ticker in StockTickers().get_all():
#     f.write('https://query1.finance.yahoo.com/v7/finance/options/{}?lang=en-US&region=US&corsDomain=finance.yahoo.com\n'.format(ticker))
