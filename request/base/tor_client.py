from stem import Signal
from stem.control import Controller

from utils.credentials import Credentials

CONTROLLER_PORT = 9051

class TorClient():

    def __init__(self):
        self.task_name = "TorClient"
        self.tor_pw = Credentials().get_tor_password()

    def connect(self):
        try:
            self.controller = Controller.from_port(port=CONTROLLER_PORT)
            self.controller.authenticate(self.tor_pw)
        except Exception, e:
            print "Error trying to connect to tor: {}".format(e)

    def disconnect(self):
        if self.controller and hasattr(self.controller, 'close'):
            self.controller.close()

    def new_nym(self):
        if self.controller.is_newnym_available():
            self.controller.signal(Signal.NEWNYM)
            return True
        return

class TorTest():

    def __init__(self):
        self.tor_client = TorClient()
        self._run()

    def _run(self):
        self.tor_client.connect()
        value = None
        print 'Waiting for New NYM'
        while value is not True:
            value = self.tor_client.new_nym()
        print 'Found New NYM'
        self.tor_client.disconnect()
        return

if __name__ == "__main__":
    TorTest()