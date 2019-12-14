# https://www.barchart.com/proxies/core-api/v1/options/chain?symbol=AAPL&fields=strikePrice%2ClastPrice%2CpercentFromLast%2CbidPrice%2Cmidpoint%2CaskPrice%2CpriceChange%2CpercentChange%2Cvolatility%2Cvolume%2CopenInterest%2CoptionType%2CdaysToExpiration%2CexpirationDate%2CtradeTime%2CsymbolCode%2CsymbolType&groupBy=optionType&expirationDate=2019-12-20&raw=1&meta=field.shortName%2Cfield.type%2Cfield.description
import random
import string
from copy import deepcopy
from datetime import datetime

from request.base.ResponseWrapper import ResponseWrapper

BASE_URL = 'https://www.barchart.com/proxies/core-api/v1/options/chain?symbol={}&fields=strikePrice%2ClastPrice%2CpercentFromLast%2CbidPrice%2Cmidpoint%2CaskPrice%2CpriceChange%2CpercentChange%2Cvolatility%2Cvolume%2CopenInterest%2CoptionType%2CdaysToExpiration%2CexpirationDate%2CtradeTime%2CsymbolCode%2CsymbolType&groupBy=optionType&raw=1&meta=field.shortName%2Cfield.type%2Cfield.description'
EXPIRATION_QUERY = '&expirationDate={}'

#fields=strikePrice,lastPrice,percentFromLast,bidPrice,midpoint,askPrice,priceChange,percentChange,volatility,volume,openInterest,optionType,daysToExpiration,expirationDate,tradeTime,symbolCode,symbolType

#strikePrice,lastPrice,theoretical,volatility,delta,gamma,rho,theta,vega,volume,openInterest,optionType,daysToExpiration,expirationDate,tradeTime,symbolCode,symbolType

# greeks referer
# https://www.barchart.com/stocks/quotes/AAPL/volatility-greeks

OPTIONS_REFERER = 'https://www.barchart.com/stocks/quotes/AAPL/options'
REFERER_EXPIRATION_QUERY = '?expiration={}'

HEADERS = {
    'accept': 'application/json',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9,pl;q=0.8',
    'cache-control': 'no-cache',
    'cookie': 'XSRF-TOKEN=eyJpdiI6IlB2TmxNb3NkbDJoU2pnZlE2MnRvMlE9PSIsInZhbHVlIjoibG9hWnAzckw4KzRUS3BTVVhwaFwvNFdOWndkU1RNK3BhU2phemx1eDhVODRtRUp2SFhYdVNrRzlZRlB0Zzl2TUsiLCJtYWMiOiJkODVkYTAxNmMxMjUxNDBjMWE2ZWQ1ZTU4ZDE1MDRkNDE5ODEwMjE4ZWNjYjQ0NzAwN2MxNmEzM2FhYjMzOGJmIn0%3D; laravel_session=eyJpdiI6IkhWdmlibEhiaXdaQWl0QnhcL2ZBMUdBPT0iLCJ2YWx1ZSI6IjZyNzJkRUs0dTRhd1JBS2hcL21VTWlwMkpRd2VHMnBNMm1ydXFBcmJwYTBSS0tLV0Rmd3hVR2VXd2x5QWJvSmduIiwibWFjIjoiYWNiNDZkODRhMWZmM2Q1NzY1NzNhN2JiMDdkYmY1ODA5MzdiOTFlYWMxM2QwMTU5MmFmZTk3MWU0OTI3MWY0ZSJ9; market=eyJpdiI6InFIbFVJckhNZXFFZk5PWmNZanI1b1E9PSIsInZhbHVlIjoiK2VKTXpMNGlGNzRRSVJZYUQyQWNPUT09IiwibWFjIjoiODIzOWQ5MGVhNjZiZjEyNDBjOGU3OTUxZjdlNTYwMzkxMTRkYWYxZjhlNWE3MjI4MzBlMzMyMmNjMDMwYmRjMyJ9',
    'pragma': 'no-cache',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36',
    'x-xsrf-token': 'eyJpdiI6IlB2TmxNb3NkbDJoU2pnZlE2MnRvMlE9PSIsInZhbHVlIjoibG9hWnAzckw4KzRUS3BTVVhwaFwvNFdOWndkU1RNK3BhU2phemx1eDhVODRtRUp2SFhYdVNrRzlZRlB0Zzl2TUsiLCJtYWMiOiJkODVkYTAxNmMxMjUxNDBjMWE2ZWQ1ZTU4ZDE1MDRkNDE5ODEwMjE4ZWNjYjQ0NzAwN2MxNmEzM2FhYjMzOGJmIn0='
}

# eyJpdiI6Inl6b2pcL0NJVEp0OW9DKzRBdmc5OFRRPT0iLCJ2YWx1ZSI6IkRtbmViSW1GMDVwWkJ6XC9xemdRaW1pMkJXdWpaUWR0SFZSUzhrR0pRR0NOSEJxd2tCWkx4TEN5TEcxaWFNRzNuIiwibWFjIjoiY2I1ZmI3ZmY4NzM5M2NkODZhZGVjOWEyOGQwYzlhYTEyNDM0ZjQxMDk4ZDcxNmRjZTQ4YjgyMTZmMGI3NDEyNSJ9
# eyJpdiI6Ik1GRm1QQ3BHU1M3SnJNelViTW5jeGc9PSIsInZhbHVlIjoiNkdha1NuZkxSSEthSG5NeTd2ZUhGanFheVdhMnByXC9hakJWZ29vSEFxNk5RNzVXZFZSSDRTVzdJUnVZdEpXMzEiLCJtYWMiOiIzZTA0YjNhMjdjZDI1ZTA1MDVjZTBiMzhjYzc4OWJiYTczOTQ4NGYzNzk0Mjk1OGJiYjBjY2RkODk1NzM4N2FhIn0=
# eyJpdiI6Ikh3Tk1DalZncW0wTHdoM00yRDZWbmc9PSIsInZhbHVlIjoiakpsZFwvRjZwQllZbVMzQ2JrcGdtZTZJU1dkR1c2SVlQUWVkeHhoUUlnZTlpczQzV3FRNm5GYk9SKzc5S1VXbkoiLCJtYWMiOiI3NTcxYjhkOWRhZWMwNDU0NmIwMjllMTlmMjFlMmJlNWRlYzk4MjMyZGU4ZTI5MTJkYmE4NzFlN2IzNmRkNGEwIn0%3D

# HEADERS = {
#     'accept': 'application/json',
#     'accept-encoding': 'gzip, deflate, br',
#     'accept-language': 'en-US,en;q=0.9,pl;q=0.8',
#     'cache-control': 'no-cache',
#     'cookie': 'XSRF-TOKEN={}; laravel_session=eyJpdiI6IkhWdmlibEhiaXdaQWl0QnhcL2ZBMUdBPT0iLCJ2YWx1ZSI6IjZyNzJkRUs0dTRhd1JBS2hcL21VTWlwMkpRd2VHMnBNMm1ydXFBcmJwYTBSS0tLV0Rmd3hVR2VXd2x5QWJvSmduIiwibWFjIjoiYWNiNDZkODRhMWZmM2Q1NzY1NzNhN2JiMDdkYmY1ODA5MzdiOTFlYWMxM2QwMTU5MmFmZTk3MWU0OTI3MWY0ZSJ9; market=eyJpdiI6InFIbFVJckhNZXFFZk5PWmNZanI1b1E9PSIsInZhbHVlIjoiK2VKTXpMNGlGNzRRSVJZYUQyQWNPUT09IiwibWFjIjoiODIzOWQ5MGVhNjZiZjEyNDBjOGU3OTUxZjdlNTYwMzkxMTRkYWYxZjhlNWE3MjI4MzBlMzMyMmNjMDMwYmRjMyJ9',
#     'pragma': 'no-cache',
#     'sec-fetch-mode': 'cors',
#     'sec-fetch-site': 'same-origin',
#     'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36',
#     'x-xsrf-token': '{}'
# }

XSRF_LENGTH = 244

# bcLightView=true; remember_web_cda0695f5f0a49e07bde136b0c83d5548b88d10f=eyJpdiI6IlBiUFg2VHZ3NnU2ZSsxVDA4UEZreVE9PSIsInZhbHVlIjoiRjc4OHREaFRVcndZcDZNMEV0Y1l4YTJyOEtKSDJOZlMzbFA2TGozUlNsWEYzT0pqSXNsY0FLUVJldUpWb3lDTXo1YVVKRXlmMUkrZjJhOWZoMFVLR2JmNnZFVmNrOVBtazI5K2lzNCtxd3F6dUlOdzRiUHFtb1ZEZUd0U0ZZNFRmQmFnSFdLdnV6U01tV085RUNFdk96c0tkajVEWVRxblwvSVpSd003aWtaZzlra1BsNGZUQUNNN1F3TTFJNmxkTiIsIm1hYyI6ImIxNzZmZTIxNjE2MDYwNDcxNDlhYWMzNGU4ZWFlY2Y1NTAxNTA4ODJiOTY1YmJkNDk1ODY0MTMwNTVlZDdhZDAifQ%3D%3D; market=eyJpdiI6Im5NTUt3TlROZlFNTHY5eWhvSkE3RFE9PSIsInZhbHVlIjoiQ1VqNmFWUGlUcyt6N29tV1p3TktHQT09IiwibWFjIjoiYjMwOWVmYzAzODk5M2NmYTk3ODVkMjUzMTUxNWZjZmJhZDYwYzVmNWEwODZmMDc5ZjE0YTRmZmM2NmQwNmZmNyJ9; laravel_token=eyJpdiI6InR2ckRNREhRakdEUHRPeUZDYmR3RGc9PSIsInZhbHVlIjoidldWNk4zdlZ6SHRXaVVseFRaeDVOVjJ5ejhTZzlGOXp3QW9RaXRya3RPTU1ac1A3TnFucG03UzBxUGUxSlMwNmZQNTlPTFdyRDRNS2NTZDhaS1ZHVjlSeTJualZFQUlYMW1DdlwvM3Q1VnhEMVY4ZUlHSVpqYlJyVG5pUW44ZFZWelVpK3l4bG1CUElIM1wvblRtVko5RWxWWVBGUHROd2s4Qk85UEpHWjZETTRmb0JzalFJRHBRWmZvTFFDYVpKeU9cL1hNdU1LNVh6M1FGYVFXSlwvR2krTUxiVkZMVDR3NFRsb1FcL0s5SE5rZ0pPQU5md1V6R1Y0dXQwOHlVNkdnYnZuSE03ZTc0RlY1NHN3WkZTcTM5NXR5dz09IiwibWFjIjoiNGVmMjZiYmZiNDljYTJhNWIzYjgwODkwY2RmYmYxOTY3NTk3NTViYjRmN2I0NWFmYzQzYjU0MTllMmU1YTQ4MCJ9; XSRF-TOKEN=eyJpdiI6IkU5TUk0TE9OYjBRZEpBM2NNejJzdnc9PSIsInZhbHVlIjoiZ2FmTGVKa1RPbEllVktcL1wvTW9jUHlhSXVWaHdUZWpxbkxGSlRZUjFnVlJwMCtpc0VWbk5Rb0JUZG9ibjVPQUVqIiwibWFjIjoiNWVlNzEyZjM4MjY4YzA3NmI4MmYwZDc2NGU0ODNmZmFlNjlhYzlkZmU1ZmU0MTlmOTRiMDNlNzc4YWI1NGRiNiJ9; laravel_session=eyJpdiI6IktaYW1pa3RCZmV0Tmtkc29ldkFvQ3c9PSIsInZhbHVlIjoieGhIclcxOHByWEE1cVc3eXkyYURteWZWNUNEbUxhTEdNRFJqZHdLQXp6bmhZOEgydDZXYjlKakJ3NiszQW9NdiIsIm1hYyI6ImZiYTdlNmY0ODkxNDY4ODViM2Q0NDc3NzdjY2UzMTRmMDQ3ODQyNDJhNDYwNzNjNDExZTg3NDc5NTYxNjg0Y2UifQ%3D%3D

class BarchartOptionsRequestException(Exception):
    pass


class BarchartOptionsRequest:

    def __init__(self, symbol, expiration_date=None):
        self.symbol = symbol
        if expiration_date is not None:
            try:
                datetime.strptime(expiration_date, '%Y-%m-%d')
            except Exception:
                raise BarchartOptionsRequestException('invalid expiration_date string')
        self.expiration_date = expiration_date

    def get_http_method(self):
        return 'GET'

    def get_headers(self):
        headers = deepcopy(HEADERS)
        referer = deepcopy(OPTIONS_REFERER)
        if self.expiration_date is not None:
            referer += deepcopy(REFERER_EXPIRATION_QUERY).format(self.expiration_date)
        headers['referer'] = referer

        # # xsrf = ''.join(random.choice(string.ascii_uppercase + string.digits + string.ascii_lowercase) for _ in range(XSRF_LENGTH))
        # xsrf = 'eyJpdiI6Ik1GRm1QQ3BHU1M3SnJNelViTW5jeGc9PSIsInZhbHVlIjoiNkdha1NuZkxSSEthSG5NeTd2ZUhGanFheVdhMnByXC9hakJWZ29vSEFxNk5RNzVXZFZSSDRTVzdJUnVZdEpXMzEiLCJtYWMiOiIzZTA0YjNhMjdjZDI1ZTA1MDVjZTBiMzhjYzc4OWJiYTczOTQ4NGYzNzk0Mjk1OGJiYjBjY2RkODk1NzM4N2FhIn1'
        # headers['cookie'] = headers['cookie'].format(xsrf)
        # headers['x-xsrf-token'] = xsrf
        return headers

    def get_url(self):
        base = deepcopy(BASE_URL)
        base = base.format(self.symbol)
        if self.expiration_date is not None:
            base += deepcopy(EXPIRATION_QUERY).format(self.expiration_date)
        return base

    @staticmethod
    def parse_response(response):
        data = {}
        if 'meta' in response.keys():
            if 'expirations' in response['meta'].keys():
                data['expirations'] = response['meta']['expirations']
            if 'isMonthly' in response['meta'].keys():
                data['isMonthly'] = response['meta']['isMonthly']

        if 'data' in response.keys():
            if 'Call' in response['data'].keys():
                calls = []
                for call in response['data']['Call']:
                    if 'raw' in call.keys():
                        calls.append(call['raw'])
                    else:
                        raise BarchartOptionsRequestException('no raw')
                data['calls'] = calls
            if 'Put' in response['data'].keys():
                puts = []
                for put in response['data']['Put']:
                    if 'raw' in put.keys():
                        puts.append(put['raw'])
                    else:
                        raise BarchartOptionsRequestException('no raw')
                data['puts'] = puts
        return data


if __name__ == '__main__':
    # a = 'eyJpdiI6IlB2TmxNb3NkbDJoU2pnZlE2MnRvMlE9PSIsInZhbHVlIjoibG9hWnAzckw4KzRUS3BTVVhwaFwvNFdOWndkU1RNK3BhU2phemx1eDhVODRtRUp2SFhYdVNrRzlZRlB0Zzl2TUsiLCJtYWMiOiJkODVkYTAxNmMxMjUxNDBjMWE2ZWQ1ZTU4ZDE1MDRkNDE5ODEwMjE4ZWNjYjQ0NzAwN2MxNmEzM2FhYjMzOGJmIn0='
    # b = 1

    import requests
    r = BarchartOptionsRequest('AAPL', '2019-12-20')
    response = requests.get(r.get_url(), headers=r.get_headers())
    rw = ResponseWrapper(response)
    BarchartOptionsRequest.parse_response(rw.get_data())