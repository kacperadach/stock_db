from datetime import date, datetime, timedelta

from logger import Logger
from app.thread import FThread
from acquisition.daily.options import get_all_options_data
from acquisition.daily.insider import get_all_insider_data


class Acquirer(FThread):

    def __init__(self):
        super(Acquirer, self).__init__()
        self.thread_name = 'Acquirer'
        self.trading_day = None
        self.history = {}


    def _run(self):
        self.history.clear()
        self.trading_date = datetime.now()
        if self.trading_date.weekday() > 4:
            Logger.log('{}: Not running Acquirer on weekend: {}'.format(self.thread_name, self.trading_date))
        else:
            if self.trading_date.hour < 16:
                Logger.log('{}: Trading day has not finished yet, {}'.format(self.thread_name, self.trading_date.time()))
            else:
                self.trading_date = self.trading_date.date()
                Logger.log('{}: using {} as trading day'.format(self.thread_name, self.trading_date))

                # options
                # Logger.log('{}: beginning stock option acquisition'.format(self.thread_name))
                # start = datetime.now()
                # found, not_found = get_all_options_data(trading_date)
                # end = datetime.now()
                # Logger.log('{}: Stock Option acquisition took {}'.format(self.thread_name, end-start))
                # Logger.log('{}: Acquired options data for {} stocks.'.format(self.thread_name, len(found)))
                # if len(not_found) > 0:
                #     Logger.log('Could not find options data for {} stocks:\n {}'.format(len(not_found), not_found), 'warning')

                found, not_found = self.complete_task(get_all_options_data)
                self.assert_complete(found, not_found)

                found, not_found = self.complete_task(get_all_insider_data)
                self.assert_complete(found, not_found)

                # insider
                # Logger.log('{}: beginning stock insider data acquisition'.format(self.thread_name))
                # start = datetime.now()
                # found, not_found = get_all_insider_data(trading_date)
                # end = datetime.now()
                # Logger.log('Stock Insider data acquisition took {}'.format(end - start))
                # Logger.log('Acquired insider stock data for {} stocks.'.format(len(found)))
                # if len(not_found) > 0:
                #     Logger.log('Could not find insider stock data for {} stocks:\n {}'.format(len(not_found), not_found), 'warning')


    def complete_task(self, task_fn):
        Logger.log('{}: beginning {} task'.format(self.thread_name, task_fn.func_name))
        self.history[task_fn.func_name] = False
        start = datetime.now()
        found, not_found = task_fn(self.trading_day)
        end = datetime.now()
        Logger.log('{}: {} task took {}'.format(self.thread_name, task_fn.func_name, end - start))
        Logger.log('{}: acquired data for {} stocks.'.format(self.thread_name, len(found)))
        return found, not_found

    def assert_complete(self, found, not_found):
        if len(not_found) > 0:
            Logger.log('Could not find options data for {} stocks:\n {}'.format(len(not_found), not_found), 'warning')

    def _sleep(self):
        # Sleep until next day
        now = datetime.now()
        tomorrow = now + timedelta(days=1)
        tomorrow = datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day, hour=16, minute=0, second=0)
        x = (tomorrow - now).total_seconds()
        self.sleep_time = x