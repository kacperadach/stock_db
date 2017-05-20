

from app.thread import FThread


class Acquirer(FThread):

    def __init__(self):
        super(Scheduler, self).__init__()
        self.thread_name = 'Acquirer'