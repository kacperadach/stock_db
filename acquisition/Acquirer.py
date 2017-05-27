from datetime import date, datetime, timedelta

from logger import Logger
from app.thread import FThread
from acquisition.daily.options import get_all_options_data


class Acquirer(FThread):

    def __init__(self):
        super(Acquirer, self).__init__()
        self.thread_name = 'Acquirer'


    def _run(self):
        trading_date = datetime.now()
        if trading_date.weekday() > 4:
            Logger.log('{}: Not running Acquirer on weekend: {}'.format(self.thread_name, trading_date))
        else:
            if trading_date.hour < 16:
                Logger.log('{}: Trading day has not finished yet, {}'.format(self.thread_name, trading_date.time()))
            else:
                trading_date = trading_date.date()
                Logger.log('{}: using {} as trading day'.format(self.thread_name, trading_date))
                Logger.log('{}: beginning stock option acquisition'.format(self.thread_name))
                found, not_found = get_all_options_data(trading_date)
                Logger.log('Acquired options data for {} stocks.'.format(len(found)))
                if len(not_found) > 0:
                    Logger.log('Could not find options data for {} stocks:\n {}'.format(len(not_found), not_found), 'warning')

    def _sleep(self):
        # Sleep until next day
        now = datetime.now()
        tomorrow = now + timedelta(days=1)
        tomorrow = datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day, hour=16, minute=0, second=0)
        x = (tomorrow - now).total_seconds()
        self.sleep_time = x