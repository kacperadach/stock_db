from copy import deepcopy
from datetime import datetime

from pytz import timezone

US_TREASURYS = {
    'US1M': 'U.S. 1 Month Treasury',
    'US3M': 'U.S. 3 Month Treasury',
    'US6M': 'U.S. 6 Month Treasury',
    'US1Y': 'U.S. 1 Year Treasury',
    'US2Y': 'U.S. 2 Year Treasury',
    'US3Y': 'U.S. 3 Year Treasury',
    'US5Y': 'U.S. 5 Year Treasury',
    'US7Y': 'U.S. 7 Year Treasury',
    'US10Y': 'U.S. 10 Year Treasury',
    'US30Y': 'U.S. 30 Year Treasury'
}

UK_GOVERNMENT_BONDS = {
    'GB3M-GB': 'Gilt 3 Month Repo Rate',
    'GB6M-GB': 'Gilt 6 Month Repo Rate',
    'GB1Y-GB': 'British 1 Year Gilt',
    'UK2Y-GB': 'British 2 Year Gilt',
    'GB3Y-GB': 'British 3 Year Gilt',
    'GB4Y-GB': 'British 4 Year Gilt',
    'UK5Y-GB': 'British 5 Year Gilt',
    'GB6Y-GB': 'British 6 Year Gilt',
    'GB7Y-GB': 'British 7 Year Gilt',
    'GB8Y-GB': 'British 8 Year Gilt',
    'GB9Y-GB': 'British 9 Year Gilt',
    'UK10Y-GB': 'British 10 Year Gilt',
    'GB15Y-GB': 'British 15 Year Gilt',
    'GB20Y-GB': 'British 20 Year Gilt',
    'UK30Y-GB': 'British 30 Year Gilt'
}

GERMANY_GOVERNMENT_BONDS = {
    'DE1Y-DE': 'Germany 1 Year Bond',
    'DE2Y-DE': 'Germany 2 Year Bond',
    'DE3Y-DE': 'Germany 3 Year Bond',
    'DE4Y-DE': 'Germany 4 Year Bond',
    'DE5Y-DE': 'Germany 5 Year Bond',
    'DE6Y-DE': 'Germany 6 Year Bond',
    'DE7Y-DE': 'Germany 7 Year Bond',
    'DE8Y-DE': 'Germany 8 Year Bond',
    'DE9Y-DE': 'Germany 9 Year Bond',
    'DE10Y-DE': 'Bund 10-YR',
    'DE20Y-DE': 'Germany 20 Year Bond',
    'DE30Y-DE': 'Germany 30 Year Bond'
}

ITALY_GOVERNMENT_BONDS = {
    'IT2Y-IT': 'Italy 2 Year Bond',
    'IT3Y-IT': 'Italy 3 Year Bond',
    'IT4Y-IT': 'Italy 4 Year Bond',
    'IT5Y-IT': 'Italy 5 Year Bond',
    'IT6Y-IT': 'Italy 6 Year Bond',
    'IT7Y-IT': 'Italy 7 Year Bond',
    'IT8Y-IT': 'Italy 8 Year Bond',
    'IT9Y-IT': 'Italy 9 Year Bond',
    'IT10Y-IT': 'Italy 10 Year Bond',
    'IT15Y-IT': 'Italy 15 Year Bond',
    'IT30Y-IT': 'Italy 30 Year Bond'
}

FRANCE_GOVERNMENT_BONDS = {
    'FR1Y-FR': 'France 1 Year Bond',
    'FR2Y-FR': 'France 2 Year Bond',
    'FR3Y-FR': 'France 3 Year Bond',
    'FR4Y-FR': 'France 4 Year Bond',
    'FR5Y-FR': 'France 5 Year Bond',
    'FR6Y-FR': 'France 6 Year Bond',
    'FR7Y-FR': 'France 7 Year Bond',
    'FR8Y-FR': 'France 8 Year Bond',
    'FR9Y-FR': 'France 9 Year Bond',
    'FR10Y-FR': 'France 10 Year Bond',
    'FR15Y-FR': 'France 15 Year Bond',
    'FR20Y-FR': 'France 20 Year Bond',
    'FR30Y-FR': 'France 30 Year Bond'
}

JAPAN_GOVERNMENT_BONDS = {
    'JP3M-JP': 'Japan 3 Month Treasury',
    'JP2Y-JP': 'Japan 2 Year Note',
    'JP3Y-JP': 'Japan 3 Year Treasury',
    'JP5Y-JP': 'Japan 5 Year Bond',
    'JP10Y-JP': 'Japan 10 Year Treasury',
    'JP15Y-JP': 'Japan 15 Year Treasury',
    'JP20Y-JP': 'Japan 20 Year Treasury',
    'JP30Y-JP': 'Japan 30 Year Treasury'
}

AUSTRALIA_GOVERNMENT_BONDS = {
    'AU1Y-AU': 'Australia 1 Year Bond',
    'AU2Y-AU': 'Australia 2 Year Bond',
    'AU4Y-AU': 'Australia 4 Year Bond',
    'AU5Y-AU': 'Australia 5 Year Bond',
    'AU10Y-AU': 'Australia 10 Year Bond',
    'AU15Y-AU': 'Australia 15 Year Bond'
}

CANADA_GOVERNMENT_BONDS = {
    'CA1M-CA': 'Canada 1 Month Treasury',
    'CA3M-CA': 'Canada 3 Month Treasury',
    'CA1Y-CA': 'Canada 1 Year Bond',
    'CA3Y-CA': 'Canada 3 Year Bond',
    'CA4Y-CA': 'Canada 4 Year Bond',
    'CA5Y-CA': 'Canada 5 Year Bond',
    'CA10Y-CA': 'Canada 10 Year Bond',
    'CA20Y-CA': 'Canada 20 Year Bond',
    'CA30Y-CA': 'Canada 30 Year Bond'
}

BRAZIL_GOVERNMENT_BONDS = {
    'BR9M-BR': 'Brazil 9 Month Bond',
    'BR1Y-BR': 'Brazil 1 Year Bond'
}

BONDS = {
    'U.S. Treasurys': US_TREASURYS,
    'U.K. Government Bonds': UK_GOVERNMENT_BONDS,
    'Germany Government Bonds': GERMANY_GOVERNMENT_BONDS,
    'Italy Government Bonds': ITALY_GOVERNMENT_BONDS,
    'France Government Bonds ': FRANCE_GOVERNMENT_BONDS,
    'Japan Government Bonds ': JAPAN_GOVERNMENT_BONDS,
    'Australia Government Bonds': AUSTRALIA_GOVERNMENT_BONDS,
    'Canada Government Bonds': CANADA_GOVERNMENT_BONDS,
    'Brazil Government Bonds': BRAZIL_GOVERNMENT_BONDS
}

BOND_COUNTRY_CODES = {
    'U.S. Treasurys': 'US',
    'U.K. Government Bonds': 'UK',
    'Germany Government Bonds': 'DE',
    'Italy Government Bonds': 'IT',
    'France Government Bonds ': 'FR',
    'Japan Government Bonds ': 'JP',
    'Australia Government Bonds': 'AU',
    'Canada Government Bonds': 'CA',
    'Brazil Government Bonds': 'BR'
}

HEADERS = {
    'Host': 'ts-api.cnbc.com',
    'Connection': 'keep-alive',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'Origin': 'https://www.cnbc.com',
    'Accept': '*/*',
    'Sec-Fetch-Site': 'same-site',
    'Sec-Fetch-Mode': 'cors',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9,pl;q=0.8'
}

REFERER_URL = 'https://www.cnbc.com/quotes/?symbol={}'

TIME_INTERVALS = ('1M', '1D')

# BASE = 'https://ts-api.cnbc.com/harmony/app/bars/US3M/1D/19191119000000/20191228000000/adjusted/EST5EDT.json'
BASE = 'https://ts-api.cnbc.com/harmony/app/bars/{}/{}/{}/{}/adjusted/EST5EDT.json'  # try utc


class BondRequestException(Exception):
    pass


class BondRequest():

    def __init__(self, bond, time_interval, start, end):
        self.bond = bond

        if time_interval not in TIME_INTERVALS:
            raise BondRequestException('Invalid time interval')
        self.time_interval = time_interval

        if not isinstance(start, datetime) or not isinstance(end, datetime):
            raise BondRequestException('Start/End need to be datetime')

        self.start = start
        self.end = end

    def get_http_method(self):
        return 'GET'

    def get_headers(self):
        headers = deepcopy(HEADERS)
        referer = deepcopy(REFERER_URL)
        headers['Referer'] = referer.format(self.bond)
        return headers

    def get_url(self):
        url = deepcopy(BASE)
        return url.format(
            self.bond,
            self.time_interval,
            self.start.strftime('%Y%m%d%H%M%S'),
            self.end.strftime('%Y%m%d%H%M%S')
        )

    @staticmethod
    def     parse_response(response):
        if int(response['responseErrorMessages'][0]['responseCode']) != 200 or len(response['responseErrorMessages']) > 1:
            return []

        date_dict = {}
        for bar in response['barData']['priceBars']:
            new_data = bar
            trade_time = bar['tradeTime']
            trade_time_millis = bar['tradeTimeinMills']

            del new_data['tradeTime']
            del new_data['tradeTimeinMills']

            # EST
            dt = datetime.strptime(trade_time, '%Y%m%d%H%M%S')
            new_data['datetime'] = dt
            new_data['timestamp'] = trade_time_millis

            dt_utc = datetime.utcfromtimestamp(trade_time_millis / 1000)
            timezone('UTC').localize(dt_utc)
            new_data['datetime_utc'] = dt_utc

            if dt_utc.date() in date_dict.keys():
                date_dict[dt_utc.date()].append(new_data)
            else:
                date_dict[dt_utc.date()] = [new_data]

        return list(date_dict.values())



def scrape_bonds():
    import requests
    from bs4 import SoupStrainer, BeautifulSoup

    base = 'https://www.cnbc.com'
    bonds = '/bonds/'

    response = requests.get(base + bonds)
    header = SoupStrainer('div', {'id': 'pageHeadNav'})
    bs = BeautifulSoup(response.text, 'html.parser', parse_only=header)
    bond_types = bs.find_all('a')

    scrape_dict = {}

    for bond_type in bond_types[0:-1]:
        link = bond_type.attrs['href']
        response = requests.get(base + link)
        bond_table = SoupStrainer('div', {'class': 'future-row'})
        bs = BeautifulSoup(response.text, 'html.parser', parse_only=bond_table)

        bond_type_name = bs.find('h3').text

        rows = bs.find_all('tr')

        bond_dict = {}
        for row in rows[1:]:
            href = row.find('a').attrs['href']

            bond_symbol = href.split('=')[1]

            response = requests.get("https:" + href)

            meta = SoupStrainer('meta', {'itemprop': 'name'})
            bs = BeautifulSoup(response.text, 'html.parser', parse_only=meta)

            bond_name = None
            for m in bs.find_all('meta'):
                if m.attrs['content'] != 'CNBC':
                    bond_name = m.attrs['content']
            bond_dict[bond_symbol] = bond_name
        scrape_dict[bond_type_name] = bond_dict

    return scrape_dict


if __name__ == '__main__':
    # now = datetime.now(timezone('EST'))
    # day_before = now - timedelta(days=1)
    # br = BondRequest('US1M', '1D', day_before, now)
    # a = 1

    sd = scrape_bonds()
    a = 1
