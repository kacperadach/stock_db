from selenium import webdriver

class Selenium():

    def __init__(self):
        pass

    def get_driver(self):
        return webdriver.PhantomJS()

if __name__ == "__main__":
    driver = Selenium().get_driver()