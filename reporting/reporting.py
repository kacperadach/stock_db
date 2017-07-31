from datetime import datetime, timedelta

from app.thread import FThread
from db.Finance import FinanceDB
from discord.webhook import DiscordWebhook

COLLECTIONS = ('BioPharmCatalyst_fda', 'BioPharmCatalyst_historical', 'commodities', 'currencies', 'financials', 'stock_historical', 'stock_insider', 'stock_options', 'symbols')

class Reporting(FThread):

    def __init__(self):
        super(Reporting, self).__init__()
        self.thread_name = 'Reporting'
        self.discord = DiscordWebhook()
        self.finance_db = FinanceDB()
        self.last_run = datetime.now()

    def _run(self):
        if datetime.now().day != self.last_run.day:
            self.generate_report()
            self.send_report()
            self.last_run = datetime.now()

    def generate_report(self):
        report = {}
        for collection in COLLECTIONS:
            self.finance_db.set_collection(collection)
            trading_date = len(list(self.finance_db.find({"trading_date": str(self.last_run.date())})))
            created_on = len(list(self.finance_db.find({"created_on": str(self.last_run.date())})))
            report[collection] = trading_date + created_on
        self.report = report

    def send_report(self):
        report = "Daily Stock_DB report for {}\n\n".format(self.last_run.date() - timedelta(days=1))
        report += "Collection | New Documents\n"
        for key, value in self.report.iteritems():
            report += "{} : {}\n".format(key, value)
        self.discord.send_report(report)
        self._log("Discord Report sent")

    def _sleep(self):
        now = datetime.now()
        tomorrow = now + timedelta(days=1)
        tomorrow_midnight = datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day)
        self.sleep_time = (tomorrow_midnight - now).total_seconds()

if __name__ == "__main__":
    Reporting().start()