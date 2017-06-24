import threading
from os import environ
import json

import requests
from stem import Signal
from stem.control import Controller

from logger import Logger

proxies = {
    'http': 'socks5://localhost:9050',
    'https': 'socks5://localhost:9050'
}

class Networking():

    def __init__(self, max_threads=50, controller_port=9051, log_progress=True, update_percent=10, threadname=''):
        self.threads = []
        self.max_retry = 5
        self.max_threads = max_threads
        self.controller_port = controller_port
        self.controller = None
        self.log_process = log_progress
        self.update_percent = update_percent
        self.threadname = threadname

    def _connect_to_controller(self):
        self.controller = Controller.from_port(port=self.controller_port)
        self.controller.authenticate(environ['TOR_PW'])

    def _disconnect_from_controller(self):
        if self.controller and hasattr(self.controller, 'close'):
            self.controller.close()

    def _log_process(self):
        if self.log_process:
            self.progress += 1
            if self.progress/float(self.num_urls) * 100 > self.last_benchmark:
                self.last_benchmark += self.update_percent
                Logger.log(str(round((float(self.progress) / self.num_urls) * 100, 2)) + '%', threadname=self.threadname)

    def worker(self, urls):
        for symbol, url in urls:
            self._log_process()
            retry = 0
            try:
                response = requests.get(url.strip(), proxies=proxies)
            except:
                continue
            while response.status_code != 200 and retry <= self.max_retry:
                retry += 1
                if self.controller.is_newnym_available():
                    self.controller.signal(Signal.NEWNYM)
                response = requests.get(url.strip(), proxies=proxies)
            try:
                data = json.loads(response.text)
            except:
                data = {}
            self.responses[symbol] = data

    def execute(self, url_list):
        self._connect_to_controller()

        thread_urls = [[] for _ in range(self.max_threads)]
        for i, url in enumerate(url_list.items()):
            thread_urls[i % self.max_threads].append(url)

        self.responses = {}
        self.threads = []
        self.progress = 0
        self.last_benchmark = 0
        self.num_urls = len(url_list)
        for i in range(self.max_threads):
            t = threading.Thread(target=self.worker, args=(thread_urls[i],))
            self.threads.append(t)
            t.start()

        for t in self.threads:
            t.join()

        self._disconnect_from_controller()
        return self.responses
