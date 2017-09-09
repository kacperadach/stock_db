from datetime import datetime

class Report():

    def __init__(self):
        self.report = {}

    def add_to_report(self, collection, increment):
        today = datetime.now().date()
        if today in self.report.keys():
            report = self.report[today]
        else:
            self.report[today] = {}
            report = self.report[today]

        if collection in report.keys():
            report[collection] += increment
        else:
            report[collection] = increment

        self.report[today] = report

    def get_report(self, date):
        return self.report.pop(date)

Reporter = Report()