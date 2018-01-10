from datetime import datetime

from stem import Signal
from stem.control import Controller
from stem.process import launch_tor_with_config

from utils.credentials import Credentials
from core.StockDbBase import StockDbBase

CONTROLLER_PORT = 9051
IP_ADDRESS_API = 'http://bot.whatismyipaddress.com/'

class TorClient(StockDbBase):

    def __init__(self):
        super(TorClient, self).__init__()
        credentials = Credentials()
        self.tor_pw = credentials.get_tor_password()
        self.tor_pw_hash = credentials.get_tor_password_hash()
        self.tor_path = credentials.get_tor_path()
        self.controller = None

    def start_tor(self):
        try:
            config = {
                'ControlPort': '9051',
                'CookieAuthentication': '1',
                'HashedControlPassword': self.tor_pw_hash
            }
            launch_tor_with_config(tor_cmd=self.tor_path, config=config, take_ownership=True)
        except OSError as e:
            self.log("Error while launching tor: {}".format(e))

    def connect(self):
        try:
            self.controller = Controller.from_port(port=CONTROLLER_PORT)
            self.controller.authenticate(self.tor_pw)
        except Exception as e:
            self.log("Error trying to connect to tor: {}".format(e))

    def disconnect(self):
        if self.controller and hasattr(self.controller, 'close'):
            self.controller.close()

    def new_nym(self):
        if self.controller:
            if self.controller.is_newnym_available():
                self.controller.signal(Signal.NEWNYM)
                self.log('Found new NYM')
                return True
        return False

    def test(self):
        try:
            self.log('Starting Tor')
            self.start_tor()
            self.log('Connecting to Tor')
            self.connect()
            from request.base.RequestClient import RequestClient
            from core.QueueItem import QueueItem
            rc = RequestClient(use_tor=True)
            response = rc.get(QueueItem(symbol='test', url=IP_ADDRESS_API, callback=map))
            if response.status_code != 200:
                raise AssertionError("Unable to query IP address api")
            ip_addr = response.response.content
            self.log("IP address: {}".format(ip_addr))
            value = None
            self.log('Waiting for New NYM')
            start = datetime.now()
            while value is not True:
                value = self.new_nym()
                if (datetime.now() - start).total_seconds() > 30:
                    break
            self.log('Found New NYM')
            response = rc.get(QueueItem(symbol='test', url=IP_ADDRESS_API, callback=map))
            if response.status_code != 200:
                raise AssertionError("Unable to query IP address api")
            assert ip_addr != response.response.content
            self.log("Different IP address: {}".format(response.response.content))
            self.log('Disconnecting from Tor')
            self.disconnect()
        except Exception as e:
            self.log_exception(e)
            raise AssertionError(e)

Tor_Client = TorClient()
