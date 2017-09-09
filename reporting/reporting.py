from datetime import datetime, timedelta

from app.thread import FThread
from discord.webhook import DiscordWebhook
from report import Reporter

COLLECTIONS = ('BioPharmCatalyst_fda', 'BioPharmCatalyst_historical', 'commodities', 'currencies', 'financials', 'stock_historical', 'stock_insider', 'stock_options', 'symbols')

class Reporting(FThread):

    def __init__(self):
        super(Reporting, self).__init__()
        self.thread_name = 'Reporting'
        self.discord = DiscordWebhook()
        self.last_run = datetime.now()

    def _run(self):
        if datetime.now().day != self.last_run.day:
            self.generate_report()
            self.send_report()
            self.last_run = datetime.now()

    def generate_report(self):
        self.report = Reporter.get_report(self.last_run.date())

    def send_report(self):
        report = "Daily Stock_DB report for {}\n\n".format(self.last_run.date())
        report += "Collection | New Documents\n"
        for key, value in self.report.iteritems():
            report += "{} : {}\n".format(key, value)
        self.discord.send_report(report)
        self._log("Discord Report sent")
        self._log(report)

    def _sleep(self):
        now = datetime.now()
        tomorrow = now + timedelta(days=1)
        tomorrow_midnight = datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day)
        self.sleep_time = (tomorrow_midnight - now).total_seconds()

if __name__ == "__main__":
    Reporting().start()