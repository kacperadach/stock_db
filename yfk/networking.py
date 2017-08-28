import urllib2
from urllib2 import HTTPError
import threading
from os import environ
import json
import logging
import sys

import requests
from stem import Signal
from stem.control import Controller

from logger import Logger

logging.getLogger('stem').setLevel(logging.WARNING)

TOR_PROXIES = {
    'http': 'socks5://localhost:9050',
    'https': 'socks5://localhost:9050'
}

class Networking():

    def __init__(self, max_threads=10, controller_port=9051, log_progress=True, update_percent=10, threadname='', request_method='requests'):
        self.threads = []
        self.max_retry = 2
        self.max_threads = max_threads
        self.controller_port = controller_port
        self.log_process = log_progress
        self.update_percent = update_percent
        self.threadname = threadname
        self.request_method = request_method
        self.controller = None

        self.use_tor = False
        if len(sys.argv) > 1:
            for i in range(1, len(sys.argv)):
                key, value = sys.argv[i].split('=')
                if key == 'use_tor':
                    if value.lower() == 'true':
                        setattr(self, key, True)
                    else:
                        setattr(self, key, False)

        if self.use_tor:
            self._connect_to_controller()

    def _connect_to_controller(self):
        try:
            self.controller = Controller.from_port(port=self.controller_port)
            self.controller.authenticate(environ['TOR_PW'])
        except Exception, e:
            print "Error trying to connect to tor: {}".format(e)

    def _disconnect_from_controller(self):
        if self.controller and hasattr(self.controller, 'close'):
            self.controller.close()

    def _log(self, msg, level='info'):
        Logger.log(msg, level=level, threadname='Networking')

    def _log_process(self):
        if self.log_process:
            self.progress += 1
            if self.progress/float(self.num_urls) * 100 > self.last_benchmark:
                self.last_benchmark += self.update_percent
                Logger.log(str(round((float(self.progress) / self.num_urls) * 100, 2)) + '%', threadname=self.threadname)

    def _request(self, url):
        if self.controller is None:
            proxies = {}
        else:
            proxies = TOR_PROXIES
        if self.request_method.lower() == 'requests':
            return requests.get(url.strip(), proxies=proxies)
        elif self.request_method.lower() == 'urllib':
            req = urllib2.Request(url.strip(), headers={'User-Agent': "Magic Browser"})
            try:
                con = urllib2.urlopen(req)
                con.status_code = con.code
            except HTTPError as e:
                e.status_code = e.code
                con = e
            return con


    def worker(self, urls):
        for symbol, url in urls:
            response = None
            retry = 0
            self._log_process()
            while response is None or (response.status_code != 200 and retry <= self.max_retry):
                try:
                    response = self._request(url)
                except requests.ConnectionError:
                    pass
                if retry > 0 and self.controller is not None and self.controller.is_newnym_available():
                    self.controller.signal(Signal.NEWNYM)
                retry += 1
            if self.request_method == 'requests':
                try:
                    data = json.loads(response.text)
                except:
                    if hasattr(response, 'text') and response.text:
                        data = response.text
                    else:
                        data = {}
            elif self.request_method == 'urllib':
                data = response.read()
            self.responses[symbol] = data

    def execute(self, url_list):
        if self.controller and self.use_tor:
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

        if self.controller and self.use_tor:
            self._disconnect_from_controller()
        return self.responses
