from os import path

from core.StockDbBase import StockDbBase
from TorClient import TorClient
from utils.credentials import Credentials

DEFAULT_NUM_TOR_INSTANCES = 3
SOCKS_PORT = 9050
CONTROL_PORT = 9051

class TorManager(StockDbBase):

    def __init__(self, num_tor_instances=DEFAULT_NUM_TOR_INSTANCES):
        super(TorManager, self).__init__()
        self.num_tor_instances = num_tor_instances
        self.tor_instances = []
        self.tor_path = Credentials().get_tor_path()

    def get_data_directory(self, x):
        return path.join(path.split(path.split(self.tor_path)[0])[0], 'Data', 'Tor{}'.format(x))

    def start_instances(self):
        self.log('Attempting to launch {} tor instances'.format(self.num_tor_instances))
        if self.tor_instances:
            self.log('Not starting additional tor instances as references already exist')
            return

        start_socks_port = SOCKS_PORT
        start_control_port = CONTROL_PORT

        for x in range(self.num_tor_instances):
            tor_client = TorClient(start_socks_port + (2 * x), start_control_port + (2 * x), self.get_data_directory(x))
            tor_client.start_tor()
            self.tor_instances.append(tor_client)

        self.log('Successfully launched {} tor instances'.format(self.num_tor_instances))

    def test(self):
        tc = self.tor_instances[0]
        self.log('Running Tor Test for SocksPort={}, ControlPort={}'.format(tc.SocksPort, tc.ControlPort))
        tc.test()

Tor_Manager = TorManager()
