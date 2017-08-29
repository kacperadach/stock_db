from Queue import Queue, Empty
from threading import Thread
from urllib2 import urlopen, Request, HTTPError
from httplib import BadStatusLine

import requests

from response_wrapper import ResponseWrapper

QUEUE_TIMEOUT = 30
CONTROLLER_PORT = 9051

class RequestQueue():

    def __init__(self, num_request_threads=10, num_retries=2, request_method='requests'):
        self.num_request_threads = num_request_threads
        self.num_retries = num_retries
        self.request_method = request_method
        self.url_queue = None
        self.output_queue = None
        self.request_threads = []

    def _request(self, url):
        response = None
        if self.request_method.lower() == 'requests':
            try:
                response = requests.get(url.strip())
            except requests.ConnectionError:
                response = None
        elif self.request_method.lower() == 'urllib':
            try:
                print url
                req = Request(url.strip(), headers={'User-Agent': "Magic Browser"})
                response = urlopen(req)
            except (HTTPError, BadStatusLine) as e:
                response = e
        return ResponseWrapper(response)

    def start(self, url_queue, output_queue):
        def execute_worker():
            self.request_threads = []
            for i in range(self.num_request_threads):
                worker = Thread(target=self.request_worker, args=(url_queue, output_queue,))
                worker.setDaemon(True)
                self.request_threads.append(worker)
                worker.start()

            for thread in self.request_threads:
                thread.join()
            print 'Finished Request Queue Worker'

        t = Thread(target=execute_worker)
        t.setDaemon(True)
        t.start()

    def request_worker(self, url_queue, output_queue):
        tickers_requested = 0
        try:
            while True:
                url_obj = url_queue.get(timeout=QUEUE_TIMEOUT)
                url = url_obj['url']
                tickers_requested += 1
                retry = 0
                response = None
                while response is None or (response.status_code != 200 and retry < self.num_retries):
                    response = self._request(url)
                    retry += 1
                url_obj['data'] = response.get_data(self.request_method)
                output_queue.put(url_obj, timeout=QUEUE_TIMEOUT)
        except Empty:
            print 'Closing Request Thread, tickers requested: {}'.format(tickers_requested)

if __name__ == "__main__":
    from acquisition.symbol.financial_symbols import Financial_Symbols
    CURRENT_OWNERSHIP = "https://fintel.io/so/us/{}"
    rq = RequestQueue(request_method='urllib')
    url_queue = Queue()
    output_queue = Queue()

    def url_input_worker(queue):
        print "url input started"
        for symbol in Financial_Symbols.get_all():
            url_obj = {"url": CURRENT_OWNERSHIP.format(symbol), "symbol": symbol, "data": None}
            queue.put(url_obj)

    t = Thread(target=url_input_worker, args=(url_queue,))
    t.start()

    rq.execute(url_queue, output_queue)

    data = []
    try:
        while 1:
            data.append(output_queue.get(timeout=1))
    except Empty:
        pass
    print data

