from bs4 import BeautifulSoup, SoupStrainer
from datetime import datetime

BASE_URL = 'https://finviz.com/quote.ashx?t={}'

class FinvizRequest():

    def __init__(self, symbol):
        self.symbol = symbol

    def get_http_method(self):
        return 'GET'

    def get_headers(self):
        return {}

    def get_url(self):
        return BASE_URL.format(self.symbol)

    @staticmethod
    def parse_response(response):
        parsed_data = {}

        stock_stats = {}
        data_table = SoupStrainer('table', {'class': 'snapshot-table2'})
        bs = BeautifulSoup(response, 'html.parser', parse_only=data_table)
        rows = bs.find_all('tr')
        for row in rows:
            tds = row.find_all('td')
            for index in range(len(tds) / 2):
                i = index * 2
                key = tds[i].text
                value = tds[i + 1].text
                if key:
                    key = key.replace('.', '')
                    stock_stats[key] = value

        parsed_data['stats'] = stock_stats

        parsed_news = []
        news_table = SoupStrainer('table', {'class': 'fullview-news-outer'})
        news_bs = BeautifulSoup(response, 'html.parser', parse_only=news_table)
        rows = news_bs.find_all('tr')

        last_date = None
        for row in rows:
            tds = row.find_all('td')
            time = tds[0].text.strip()
            try:
                parsed_time = datetime.strptime(time, '%b-%d-%y %I:%M%p')
            except ValueError:
                parsed_time = datetime.strptime(time, '%I:%M%p')
                parsed_time = datetime(day=last_date.day, month=last_date.month, year=last_date.year, hour=parsed_time.hour, minute=parsed_time.minute)
            last_date = parsed_time
            news = tds[1]
            link = news.find('a').attrs['href']
            source = news.find('span').text
            parsed_news.append({
                'datetime': parsed_time,
                'link': link,
                'source': source
            })

        parsed_data['news'] = parsed_news

        parsed_ratings = []
        ratings_table = SoupStrainer('table', {'class': 'fullview-ratings-outer'})
        ratings_bs = BeautifulSoup(response, 'html.parser', parse_only=ratings_table)
        tables = ratings_bs.find_all('table')

        tables = tables[1:]

        for table in tables:
            tds = table.find_all('td')
            date = datetime.strptime(tds[0].text, '%b-%d-%y')
            rating = tds[1].text
            rated_by = tds[2].text
            from_to = tds[3].text
            if len(from_to.split(' ')) == 3:
                parsed_from_to = {'from': from_to.split(' ')[0], 'to': from_to.split(' ')[2]}
            else:
                parsed_from_to = {'to': from_to}

            price_target = tds[4].text
            if len(price_target.split(' ')) == 3:
                parsed_price_target = {'from': price_target.split(' ')[0], 'to': price_target.split(' ')[2]}
            else:
                parsed_price_target = {'to': price_target}

            parsed_ratings.append({
                'datetime': date,
                'rating': rating,
                'rated_by': rated_by,
                'from_to': parsed_from_to,
                'price_target': parsed_price_target
            })

        parsed_data['ratings'] = parsed_ratings

        description_tr = SoupStrainer('tr', {'class': 'table-light3-row'})
        description_bs = BeautifulSoup(response, 'html.parser', parse_only=description_tr)
        parsed_data['description'] = description_bs.find('tr').text


        parsed_insider = []
        insider_table = SoupStrainer('table', {'class': 'body-table'})
        insider_bs = BeautifulSoup(response, 'html.parser', parse_only=insider_table)
        rows = insider_bs.find_all('tr')[1:]

        for row in rows:
            tds = row.find_all('td')
            entity = tds[0].text
            relationship = tds[1].text
            # date_executed = datetime.strptime(tds[2].text, '%b %d')
            date_executed = tds[2].text
            transaction = tds[3].text
            cost = tds[4].text
            num_shares = tds[5].text
            dollar_value = tds[6].text
            shares_total = tds[7].text
            sec_form = tds[8].find('a')
            # sec_form_filed_at = datetime.strptime(sec_form.text, '%b %d %I:%M %p')
            sec_form_filed_at = sec_form.text
            sec_form_link = sec_form.attrs['href']
            parsed_insider.append({
                'entity': entity,
                'relationship': relationship,
                'date_executed': date_executed,
                'transaction': transaction,
                'cost': cost,
                'num_shares': num_shares,
                'dollar_value': dollar_value,
                'shares_total': shares_total,
                'sec_form_filed_at': sec_form_filed_at,
                'sec_form_link': sec_form_link
            })

        parsed_data['insider'] = parsed_insider
        return parsed_data
