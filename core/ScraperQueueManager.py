from time import sleep
from Queue import Queue
from threading import Thread, Event
from datetime import datetime, timedelta

from pytz import timezone

from acquisition.scrapers.FuturesScraper import FuturesScraper, Futures1mScraper
from acquisition.scrapers.IndexLiveScraper import IndexLiveScraper
from acquisition.scrapers.MarketWatchRequestLiveScraper import MarketWatchRequestLiveScraper
from acquisition.scrapers.RandomMarketWatchSymbols import RandomMarketWatchSymbols
from acquisition.scrapers.Stocks import StockScraper
from acquisition.scrapers.Symbols import SymbolScraper
from acquisition.scrapers.ETFSymbols import ETFSymbolScraper
from acquisition.scrapers.Forex import ForexScraper
# from acquisition.scrapers.MarketWatchFutures import MarketWatchFuturesScraper
from acquisition.scrapers.MarketWatchSymbols import MarketWatchSymbols
from acquisition.scrapers.MarketWatchScraper import MarketWatchScraper
from acquisition.scrapers.MarketWatchHistoricalScraper import MarketWatchHistoricalScraper
from acquisition.scrapers.MarketWatchLiveScraper import MarketWatchLiveScraper
from acquisition.scrapers.USTreasuryScraper import USTreasuryScraper
from acquisition.scrapers.MarketWatchSymbolsV2 import MarketWatchSymbolsV2
from core.QueueItem import QueueItem
from core.ScraperQueue import ScraperQueue
from core.data.ScraperRepository import Scraper_Repository
from request.base.RequestClient import RequestClient
from StockDbBase import StockDbBase
from core.Counter import Counter
from request.base.TorManager import Tor_Manager

URL_THREADS = 10
OUTPUT_THREADS = 3
REQUEST_QUEUE_SIZE = 20
OUTPUT_QUEUE_SIZE = 10
QUEUE_LOG_FREQ_SEC = 10
INPUT_REQUEST_DELAY = 0.01

"""
This class is in charge of:
 - creating the request_queue and output_queue
 - creating the request and output threads
 - populating the request_queue
"""

class ScraperQueueManager(StockDbBase):

    def __init__(self, use_tor=True):
        super(ScraperQueueManager, self).__init__()

        self.priority_scrapers = (MarketWatchRequestLiveScraper(), IndexLiveScraper(), FuturesScraper(), )
        self.scrapers = (RandomMarketWatchSymbols(), MarketWatchSymbolsV2(), MarketWatchHistoricalScraper())
        self.request_queue = ScraperQueue(REQUEST_QUEUE_SIZE)
        self.output_queue = Queue(maxsize=OUTPUT_QUEUE_SIZE)
        self.request_counter = Counter()
        self.successful_request_counter = Counter()
        self.failed_request_counter = Counter()
        self.event = Event()
        self.use_tor = use_tor
        self.tor_manager = Tor_Manager if use_tor else None

    def start(self):
        for x in range(URL_THREADS):
            args = (self.tor_manager.tor_instances[x % self.tor_manager.num_tor_instances],) if self.use_tor else ()
            t = Thread(target=self.request_thread_worker, args=args)
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
                allNone = True
                for scraper in self.priority_scrapers:
                    request_queue_input = scraper.get_next_input()
                    if request_queue_input:
                        allNone = False
                        self.request_queue.put(request_queue_input)
                        sleep(INPUT_REQUEST_DELAY)

                # only move on to other scrapers if all priority scraper outputs are None
                if allNone is True:
                    for scraper in self.scrapers:
                        request_queue_input = scraper.get_next_input()
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
        now = datetime.utcnow()
        while 1:
            if datetime.utcnow() - timedelta(seconds=QUEUE_LOG_FREQ_SEC) > now:
                new_now = datetime.utcnow()
                rps = float(self.request_counter.get()) / (new_now - now).total_seconds()
                rps = round(rps, 4)
                data = {
                    'datetime_utc': new_now,
                    'request_queue_size': self.request_queue.get_size(),
                    'output_queue_size': self.output_queue.qsize(),
                    'requests': self.request_counter.get(),
                    'successful_requests': self.successful_request_counter.get(),
                    'failed_requests': self.failed_request_counter.get(),
                    'requests_per_second': rps
                }
                Scraper_Repository.save_request_interval(data)
                self.log('Request Queue Size: {}'.format(data['request_queue_size']))
                self.log('Output Queue Size: {}'.format(data['output_queue_size']))
                self.log('Requests/sec: {}'.format(rps))
                self.log('Successful/Failed requests {}/{}'.format(self.successful_request_counter.get(), self.failed_request_counter.get()))
                self.request_counter.reset()
                self.successful_request_counter.reset()
                self.failed_request_counter.reset()
                now = new_now


    def request_thread_worker(self, tor_client=None):
        request_client = RequestClient(use_tor=bool(tor_client), tor_client=tor_client)
        try:
            while 1:
                queue_item = self.request_queue.get()
                if queue_item.get_http_method() == 'GET':
                    response = request_client.get(queue_item.get_url(), headers=queue_item.get_headers())
                else:
                    response = request_client.post(queue_item.get_url(), queue_item.get_body(), headers=queue_item.get_headers())
                self.request_counter.increment()
                self.log(queue_item.get_metadata())
                if response.status_code == 200:
                    self.successful_request_counter.increment()
                else:
                    self.failed_request_counter.increment()
                queue_item.add_response(response)
                self.output_queue.put(queue_item)
        except Exception as e:
            self.log_queue_item(queue_item)
            self.log_exception(e)
            self.event.set()

    def output_thread_worker(self):
        try:
            while 1:
                queue_item = self.output_queue.get(block=True)
                if not isinstance(queue_item, QueueItem):
                    raise AssertionError('need queue_item in process_data')
                start = datetime.utcnow()
                queue_item.callback(queue_item)
                seconds_took = (datetime.utcnow() - start).total_seconds()
                if seconds_took > 5:
                    self.log('Slow output processing for metadata: {} - took {} seconds'.format(queue_item.get_metadata(), seconds_took), level='warn')
        except Exception as e:
            self.log_exception(e)
            self.event.set()
