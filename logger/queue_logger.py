from threading import Thread
import time

from logger import AppLogger

class QueueLoggerError(Exception):
    pass

class QueueLogger():

    def __init__(self, log_time_interval=30):
        self.name = 'QueueLogger'
        self.running = False
        self.log_time_interval = log_time_interval
        self.log_thread = None
        self.logger = AppLogger()

    def _log(self, msg, level='info'):
        self.logger.log(msg, level=level, threadname=self.name)

    def start_logging(self, queues):
        for _, queue in queues.items():
            if not hasattr(queue, 'qsize'):
                raise QueueLoggerError('Queue supplied has no qsize method')

        self.running = True
        self.log_thread = Thread(target=self.worker, args=(queues,))
        self.log_thread.start()

    def stop_logging(self):
        self.running = False

    def worker(self, queues):
        while self.running:
            for name, queue in queues.items():
                self._log("{} Size: {}".format(name, queue.qsize()))
            time.sleep(self.log_time_interval)
