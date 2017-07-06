from datetime import datetime, timedelta
import os

from selenium import webdriver
from bs4 import BeautifulSoup

from db import FinanceDB
from logger import Logger
from discord.webhook import DiscordWebhook

FDA_CALENDAR = 'https://www.biopharmcatalyst.com/calendars/fda-calendar'
HIST_CATALYST_CALENDAR = 'https://www.biopharmcatalyst.com/calendars/historical-catalyst-calendar'

CWD = os.path.dirname(os.path.realpath(__file__))
LAST_CHECKED_PATH = os.path.join(CWD, 'last_checked.txt')

class BioPharmCatalyst():

    def __init__(self):
        self.task_name = 'BioPharmCatalyst'
        self.finance_db = FinanceDB()
        self.discord = DiscordWebhook()

    def _log(self, msg, level='info'):
        Logger.log(msg, level=level, threadname=self.task_name)

    def last_checked(self):
        if os.path.exists(LAST_CHECKED_PATH):
            f = open(LAST_CHECKED_PATH, 'r')
            lines = f.readlines()
            if len(lines) > 0:
                last_checked = lines[0]
                return datetime.strptime(last_checked.strip(), '%Y-%m-%d %H-%M')
        return None

    def update_last_checked(self):
        f = open(LAST_CHECKED_PATH, 'w')
        f.write(datetime.now().strftime('%Y-%m-%d %H-%M'))

    def start(self):
        self.lc = self.last_checked()
        if self.lc is None or (datetime.now() - self.lc).total_seconds() > 14400:
            self.driver = webdriver.PhantomJS("C:/Users/Kacper/AppData/Roaming/npm/node_modules/phantomjs-prebuilt/lib/phantom/bin/phantomjs")
            self.get_fda_calendar()
            self._log('FDA Calendar Parsed Successfully: {} new events'.format(self.found))
            self.get_historical_catalyst_calendar()
            self._log('Historical Catalyst Calendar Parsed Successfully: {} new events'.format(self.found))
            self.update_last_checked()
            self.driver.quit()
        else:
            self._log('BioPharmCatalyst Calendars checked less than 4 hours ago, not checking')

    def sleep_time(self):
        return ((self.last_checked() + timedelta(hours=4)) - datetime.now()).total_seconds()

    def get_fda_calendar(self):
        self.found = 0
        self.driver.get(FDA_CALENDAR)
        self.fda_calendar = BeautifulSoup(self.driver.page_source, "html.parser")

        table = self.fda_calendar.findChildren('table')[0]
        rows = table.findChildren('tr')[1:]

        fda_events = []
        for row in rows:
            try:
                table_data = map(lambda x: x.text, row.findChildren('td'))
                symbol = table_data[0].strip()
                _, drug, drug_description, _ = table_data[2].split('\n')
                stage = table_data[3].replace('\n', '').strip()
                date, event_description = filter(bool, table_data[4].split('\n'))
                fda_events.append({
                    'symbol': symbol,
                    'drug': drug,
                    'drug_description': drug_description,
                    'stage': stage,
                    'date': date,
                    'event_description': event_description
                })
            except Exception as e:
                self._log('Error in FDA calendar parsing: {}'.format(e))
                if Logger.env == 'prod':
                    self.discord.send_message('Error in BioPharmCatalyst FDA calendar parsing: {}'.format(e))

        self.finance_db.set_collection('BioPharmCatalyst_fda')
        for event in fda_events:
            if len(list(self.finance_db.find(event))) == 0:
                event['created_on'] = str(datetime.now().date())
                self.found += 1
                if Logger.env == 'prod':
                    self.discord.alert_BioPharmCatalyst_fda(event)
                self.finance_db.insert_one(event)

    def get_historical_catalyst_calendar(self):
        self.found = 0
        self.driver.get(HIST_CATALYST_CALENDAR)
        self.fda_calendar = BeautifulSoup(self.driver.page_source, "html.parser")

        table = self.fda_calendar.findChildren('table')[0]
        rows = table.findChildren('tr')[1:]

        fda_events = []
        for row in rows:
            try:
                table_data = map(lambda x: x.text, row.findChildren('td'))
                if not len(table_data) > 3:
                    continue
                symbol = table_data[0].strip()
                _, drug, drug_description, _ = table_data[1].split('\n')
                stage = table_data[2].replace('\n', '').strip()
                date, event_description = filter(bool, table_data[3].split('\n'))
                fda_events.append({
                    'symbol': symbol,
                    'drug': drug,
                    'drug_description': drug_description,
                    'stage': stage,
                    'date': date,
                    'event_description': event_description
                })
            except Exception as e:
                self._log('Error in Historical Catalyst Calendar parsing: {}'.format(e))
                if Logger.env == 'prod':
                    self.discord.send_message('Error in BioPharmCatalyst Historical Catalyst Calendar parsing: {}'.format(e))

        self.finance_db.set_collection('BioPharmCatalyst_historical')
        for event in fda_events:
            if len(list(self.finance_db.find(event))) == 0:
                event['created_on'] = str(datetime.now().date())
                self.found += 1
                if Logger.env == 'prod':
                    self.discord.alert_BioPharmCatalyst_catalyst(event)
                self.finance_db.insert_one(event)

if __name__ == "__main__":
    a = BioPharmCatalyst()
    a.start()
