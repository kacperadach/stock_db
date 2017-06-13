import threading
from os import environ
import json
from datetime import datetime

import requests
from stem import Signal
from stem.control import Controller

proxies = {
    'http': 'socks5://localhost:9050',
    'https': 'socks5://localhost:9050'
}

class Networking():

    def __init__(self, max_threads=10, controller_port=9051, max_retry=3):
        self.threads = []
        self.max_retry = 5
        self.max_threads = max_threads
        self.controller_port = controller_port
        self.controller = None

    def _connect_to_controller(self):
        self.controller = Controller.from_port(port=self.controller_port)
        self.controller.authenticate(environ['TOR_PW'])

    def _disconnect_from_controller(self):
        if self.controller and hasattr(self.controller, 'close'):
            self.controller.close()

    def worker(self, urls, num):
        for url in urls:
            retry = 0
            print url
            try:
                response = requests.get(url.strip(), proxies=proxies)
            except:
                continue
            while response.status_code != 200 and retry <= self.max_retry:
                retry += 1
                if self.controller.is_newnym_available():
                    print 'New NYM'
                    self.controller.signal(Signal.NEWNYM)
                print 'Retrying ' + url
                response = requests.get(url, proxies=proxies)
            try:
                data = json.loads(response.text)
            except:
                data = {}
            self.responses[num].append(data)


    def execute(self, url_list):
        self._connect_to_controller()

        thread_urls = [[] for i in range(self.max_threads)]
        for i, url in enumerate(url_list):
            thread_urls[i % self.max_threads].append(url)


        self.responses = [[] for i in range(self.max_threads)]
        self.threads = []
        for i in range(self.max_threads):
            t = threading.Thread(target=self.worker, args=(thread_urls[i],i,))
            self.threads.append(t)
            t.start()

        for t in self.threads:
            t.join()

        self._disconnect_from_controller()
        return self.responses


urls = map(lambda x: x.strip('\n'), open('url.txt', 'r').readlines())
n = Networking(max_threads=200)
start = datetime.now()
n.execute(urls)
end = datetime.now()
print end -start

n = Networking(max_threads=100)
start = datetime.now()
responses = n.execute(urls)
end = datetime.now()
print end -start




