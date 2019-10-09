from datetime import datetime, timedelta
import os
import urllib2

from bs4 import BeautifulSoup

from db.Finance import Finance_DB
from discord.webhook import DiscordWebhook

FDA_CALENDAR = 'https://www.biopharmcatalyst.com/calendars/fda-calendar'
HIST_CATALYST_CALENDAR = 'https://www.biopharmcatalyst.com/calendars/historical-catalyst-calendar'

CWD = os.path.dirname(os.path.realpath(__file__))
LAST_CHECKED_PATH = os.path.join(CWD, 'last_checked.txt')

FDA_COLLECTION = 'BioPharmCatalyst_fda'
HISTORICAL_COLLECTION = 'BioPharmCatalyst_historical'

class BioPharmCatalyst():

    def __init__(self):
        self.task_name = 'BioPharmCatalyst'
        self.finance_db = Finance_DB
        self.discord = DiscordWebhook()
        self.fda_calendar = None
        self.last_checked = None

    def _log(self, msg, level='info'):
        pass

    def start(self):
        if self.last_checked is None or (datetime.now() - self.last_checked).total_seconds() > 14400:
            self.get_fda_calendar()
            self._log('FDA Calendar Parsed Successfully: {} new events'.format(self.found))
            self.get_historical_catalyst_calendar()
            self._log('Historical Catalyst Calendar Parsed Successfully: {} new events'.format(self.found))
            self.last_checked = datetime.now()
        else:
            self._log('BioPharmCatalyst Calendars checked less than 4 hours ago, not checking')

    def sleep_time(self):
        return ((self.last_checked + timedelta(hours=4)) - datetime.now()).total_seconds()

    def get_fda_calendar(self):
        self.found = 0

        req = urllib2.Request('https://www.biopharmcatalyst.com/calendars/fda-calendar', headers={'User-Agent': "Magic Browser"})
        con = urllib2.urlopen(req)

        page_source = con.read()
        self.fda_calendar = BeautifulSoup(page_source, "html.parser")

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
                    self.discord.alert_error(self.task_name, 'Error in FDA calendar parsing: {}'.format(e))

        for event in fda_events:
            if len(list(self.finance_db.find(event, FDA_COLLECTION))) == 0:
                event['created_on'] = str(datetime.now().date())
                self.found += 1
                if Logger.env == 'prod':
                    self.discord.alert_BioPharmCatalyst_fda(event)
                self.finance_db.insert_one(event, FDA_COLLECTION)

    def get_historical_catalyst_calendar(self):
        self.found = 0

        req = urllib2.Request('https://www.biopharmcatalyst.com/calendars/fda-calendar', headers={'User-Agent': "Magic Browser"})
        con = urllib2.urlopen(req)

        page_source = con.read()
        self.fda_calendar = BeautifulSoup(page_source, "html.parser")

        table = self.fda_calendar.findChildren('table')[0]
        rows = table.findChildren('tr')[1:]

        fda_events = []
        for row in rows:
            try:
                table_data = map(lambda x: x.text, row.findChildren('td'))
                if not len(table_data) > 3:
                    continue
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
                self._log('Error in Historical Catalyst Calendar parsing: {}'.format(e))
                if Logger.env == 'prod':
                    self.discord.alert_error(self.task_name, 'Error in BioPharmCatalyst Historical Catalyst Calendar parsing: {}'.format(e))

        documents = []
        for event in fda_events:
            if len(list(self.finance_db.find(event, HISTORICAL_COLLECTION))) == 0:
                event['created_on'] = str(datetime.now().date())
                self.found += 1
                if Logger.env == 'prod':
                    self.discord.alert_BioPharmCatalyst_catalyst(event)
                documents.append(event)
        if documents:
            self.finance_db.insert_many(documents, HISTORICAL_COLLECTION)

if __name__ == "__main__":
    a = BioPharmCatalyst()
    a.start()
