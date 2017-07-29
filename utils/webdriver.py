from os import path

from selenium import webdriver

CWD = path.dirname(path.realpath(__file__))

PHANTOM_JS_PATH = path.join(CWD, 'phantomjs', 'lib', 'phantom', 'bin', 'phantomjs')

class Selenium():

    def __init__(self):
        pass

    def get_driver(self):
        return webdriver.PhantomJS(executable_path=PHANTOM_JS_PATH)