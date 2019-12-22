from datetime import datetime
import logging

import stem
from stem import Signal
from stem.control import Controller
from stem.process import launch_tor_with_config

from utils.credentials import Credentials
from core.StockDbBase import StockDbBase
from request.base.RequestClient import RequestClient

IP_ADDRESS_API = 'http://bot.whatismyipaddress.com/'

logging.getLogger(stem.__name__).setLevel('CRITICAL')


class TorClient(StockDbBase):
    def __init__(self, SocksPort, ControlPort, DataDirectory):
        super(TorClient, self).__init__()
        self.SocksPort = SocksPort
        self.ControlPort = ControlPort
        self.DataDirectory = DataDirectory
        credentials = Credentials()
        self.tor_pw = credentials.get_tor_password()
        self.tor_pw_hash = credentials.get_tor_password_hash()
        self.tor_path = credentials.get_tor_path()
        self.controller = None

    def start_tor(self):
        config = {'SocksPort': str(self.SocksPort), 'ControlPort': str(self.ControlPort), 'DataDirectory': str(self.DataDirectory), 'CookieAuthentication': '1',
            'HashedControlPassword': self.tor_pw_hash}
        launch_tor_with_config(tor_cmd=self.tor_path, config=config, take_ownership=True)
        self.log('Successfully launched tor, SocksPort={}, ControlPort={}, DataDirectory={}'.format(self.SocksPort, self.ControlPort, self.DataDirectory))

    def connect(self):
        self.controller = Controller.from_port(port=self.ControlPort)
        self.controller.authenticate(self.tor_pw)

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
            self.log('Connecting to Tor')
            self.connect()

            rc = RequestClient(use_tor=True, tor_client=self)
            response = rc.get(IP_ADDRESS_API)
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
            response = rc.get(IP_ADDRESS_API)
            if response.status_code != 200:
                raise AssertionError("Unable to query IP address api")
            assert ip_addr != response.response.content
            self.log("Different IP address: {}".format(response.response.content))
            self.log('Disconnecting from Tor')
            self.disconnect()
        except Exception as e:
            self.log_exception(e)
            raise AssertionError(e)
