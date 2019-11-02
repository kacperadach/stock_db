from logger import AppLogger

LOG_PERCENT = 5

class BaseAcquisition():

    def __init__(self, task_name):
        self.task_name = task_name
        self.logger = AppLogger()

    def _log(self, msg, level='info'):
        self.logger.log(msg, level=level, threadname=self.task_name)

    def _log_process(self):
        if len(self.symbols) > 0:
            progress = (float(self.counter) / len(self.symbols)) * 100
            if  progress > self.last_benchmark:
                self._log(str(round(float(progress), 2)) + '%')
                self.last_benchmark += LOG_PERCENT