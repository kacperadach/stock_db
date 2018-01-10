from Queue import Queue
from threading import Thread, Event

from acquisition.scrapers.Stocks import StockScraper
from core.RequestQueue import RequestQueue
from request.base.RequestClient import RequestClient
from StockDbBase import StockDbBase

URL_THREADS = 100
OUTPUT_THREADS = 10
MAX_QUEUE_SIZE = 5000

"""
This class is in charge of:
 - creating the request_queue and output_queue
 - creating the request and output threads
 - populating the request_queue
"""

class ScraperQueueManager(StockDbBase):

    def __init__(self, use_tor=True):
        super(ScraperQueueManager, self).__init__()
        self.scrapers = (StockScraper(),)
        self.request_queue = RequestQueue(MAX_QUEUE_SIZE)
        self.output_queue = Queue(maxsize=MAX_QUEUE_SIZE)
        self.request_client = RequestClient(use_tor=use_tor)

    def start(self):
        self.event = Event()
        for _ in range(URL_THREADS):
            t = Thread(target=self.request_thread_worker)
            t.setDaemon(True)
            t.start()
        self.log("Created {} request threads".format(URL_THREADS))

        for _ in range(OUTPUT_THREADS):
            t = Thread(target=self.output_thread_worker)
            t.setDaemon(True)
            t.start()
        self.log("Created {} output threads".format(OUTPUT_THREADS))

        try:
            while not self.event.is_set():
                for task in self.scrapers:
                    request_queue_input = task.get_next_input()
                    if request_queue_input:
                        self.request_queue.put(request_queue_input)
        except Exception as e:
            self.log_exception(e)

    def request_thread_worker(self):
        try:
            while 1:
                queue_item = self.request_queue.get()
                response = self.request_client.get(queue_item)
                self.log(queue_item.get_url())
                queue_item.add_response(response)
                self.output_queue.put(queue_item)
        except Exception as e:
            self.log_exception(e)
            self.event.set()

    def output_thread_worker(self):
        try:
            while 1:
                queue_item = self.output_queue.get(block=True)
                queue_item.callback(queue_item, self.request_queue)
        except Exception as e:
            self.log_exception(e)
            self.event.set()
