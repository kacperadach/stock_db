from datetime import datetime

from app.thread import FThread
from acquisition.daily.options import OptionsAcquisition
from acquisition.daily.insider import InsiderAcquisition
from acquisition.daily.biopharmcatalyst import BioPharmCatalyst

class Acquirer(FThread):

    def __init__(self):
        super(Acquirer, self).__init__()
        self.thread_name = 'Acquirer'
        self.trading_day = None
        self.tasks = [OptionsAcquisition(), InsiderAcquisition(), BioPharmCatalyst()]

    def _run(self):
        self.trading_date = datetime.now()
        for task in self.tasks:
            self._log('beginning {} task'.format(task.task_name))
            start = datetime.now()
            task.trading_date = self.trading_date
            task.start()
            end = datetime.now()
            self._log('{} task took {}'.format(task.task_name, end - start))

    def _sleep(self):
        self.sleep_time = min(map(lambda x: x.sleep_time(), self.tasks))

if __name__ == "__main__":
    Acquirer().start()