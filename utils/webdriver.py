from os import path

from selenium import webdriver

CWD = path.dirname(path.realpath(__file__))

class Selenium():

    def __init__(self):
        pass

    def get_driver(self):
        return webdriver.PhantomJS()

if __name__ == "__main__":
    Selenium().get_driver()