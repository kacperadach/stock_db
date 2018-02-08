from time import sleep
from Queue import Queue
from threading import Thread, Event

from acquisition.scrapers.Stocks import StockScraper
from acquisition.scrapers.Symbols import SymbolScraper
from acquisition.scrapers.ETFSymbols import ETFSymbolScraper
from acquisition.scrapers.Forex import ForexScraper
from core.ScraperQueue import ScraperQueue
from request.base.RequestClient import RequestClient
from StockDbBase import StockDbBase
from core.Counter import Counter
from request.base.TorManager import Tor_Manager

URL_THREADS = 100
OUTPUT_THREADS = 30
REQUEST_QUEUE_SIZE = 5000
OUTPUT_QUEUE_SIZE = 1000
QUEUE_LOG_FREQ_SEC = 10
INPUT_REQUEST_DELAY = 0.01

"""
This class is in charge of:
 - creating the request_queue and output_queue
 - creating the request and output threads
 - populating the request_queue
"""

class ScraperQueueManager(StockDbBase):

    def __init__(self):
        super(ScraperQueueManager, self).__init__()
        # self.scrapers = (ForexScraper(),)
        self.scrapers = (StockScraper(), SymbolScraper(), ETFSymbolScraper(), ForexScraper())
        self.request_queue = ScraperQueue(REQUEST_QUEUE_SIZE)
        self.output_queue = Queue(maxsize=OUTPUT_QUEUE_SIZE)
        self.request_counter = Counter()
        self.successful_request_counter = Counter()
        self.failed_request_counter = Counter()
        self.event = Event()
        self.tor_manager = Tor_Manager

    def start(self):
        for x in range(URL_THREADS):
            t = Thread(target=self.request_thread_worker, args=(self.tor_manager.tor_instances[x % self.tor_manager.num_tor_instances],))
            t.setDaemon(True)
            t.start()
        self.log("Created {} request threads".format(URL_THREADS))

        for _ in range(OUTPUT_THREADS):
            t = Thread(target=self.output_thread_worker)
            t.setDaemon(True)
            t.start()
        self.log("Created {} output threads".format(OUTPUT_THREADS))

        self.launch_queue_logger()
        try:
            while not self.event.is_set():
                for task in self.scrapers:
                    request_queue_input = task.get_next_input()
                    if request_queue_input:
                        self.request_queue.put(request_queue_input)
                sleep(INPUT_REQUEST_DELAY)
        except Exception as e:
            self.log_exception(e)

    def launch_queue_logger(self):
        self.queue_logger = Thread(target=self.log_queue_stats)
        self.queue_logger.setDaemon(True)
        self.queue_logger.start()

    def log_queue_stats(self):
        while 1:
            self.log('Request Queue Size: {}'.format(self.request_queue.get_size()))
            self.log('Output Queue Size: {}'.format(self.output_queue.qsize()))
            self.log('Requests/sec: {}'.format(float(self.request_counter.get())/QUEUE_LOG_FREQ_SEC))
            self.log('Successful/Failed requests {}/{}'.format(self.successful_request_counter.get(), self.failed_request_counter.get()))
            self.request_counter.reset()
            self.successful_request_counter.reset()
            self.failed_request_counter.reset()
            sleep(QUEUE_LOG_FREQ_SEC)

    def request_thread_worker(self, tor_client):
        request_client = RequestClient(use_tor=True, tor_client=tor_client)
        try:
            while 1:
                queue_item = self.request_queue.get()
                if queue_item.get_http_method() == 'GET':
                    response = request_client.get(queue_item.get_url(), headers=queue_item.get_headers())
                else:
                    response = request_client.post(queue_item.get_url(), queue_item.get_body(), headers=queue_item.get_headers())
                self.request_counter.increment()
                self.log(queue_item.get_symbol())
                if response.status_code == 200:
                    self.successful_request_counter.increment()
                else:
                    self.failed_request_counter.increment()
                queue_item.add_response(response)
                self.output_queue.put(queue_item)
        except Exception as e:
            self.log_exception(e)
            self.event.set()

    def output_thread_worker(self):
        try:
            while 1:
                queue_item = self.output_queue.get(block=True)
                queue_item.callback(queue_item)
        except Exception as e:
            self.log_exception(e)
            self.event.set()
