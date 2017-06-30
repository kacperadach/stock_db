from selenium import webdriver
from bs4 import BeautifulSoup


FDA_CALENDAR = 'https://www.biopharmcatalyst.com/calendars/fda-calendar'

class BioPharmCatalyst():

    def __init__(self):
        pass

    def start(self):
        self.driver = webdriver.PhantomJS()
        self.get_fda_calendar()

    def get_fda_calendar(self):
        self.driver.get(FDA_CALENDAR)
        self.fda_calendar = BeautifulSoup(self.driver.page_source)

        a = 1



    def sleep(self):
        pass


a = BioPharmCatalyst()
a.start()
