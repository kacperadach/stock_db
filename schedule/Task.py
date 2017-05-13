

class ScheduleTaskError(Exception):
    pass

class ScheduleTask():

    def __init__(self, date):
        self.date = date

    def run(self):
        pass