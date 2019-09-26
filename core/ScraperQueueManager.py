from time import sleep
from Queue import Queue, Empty
from threading import Thread, Event
from datetime import datetime, timedelta
import multiprocessing

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
from core.OutputWorkerProcess import output_worker_process
from core.QueueItem import QueueItem
from core.ScraperQueue import ScraperQueue
from core.data.ScraperRepository import Scraper_Repository
from request.base.RequestClient import RequestClient
from StockDbBase import StockDbBase
from core.Counter import Counter
from request.base.TorManager import Tor_Manager

URL_THREADS = 10
OUTPUT_PROCESSES = 4
REQUEST_QUEUE_SIZE = 20
OUTPUT_QUEUE_SIZE = 100
QUEUE_LOG_FREQ_SEC = 10
INPUT_REQUEST_DELAY = 0.1

"""
This class is in charge of:
 - creating the request_queue and output_queue
 - creating the request and output threads
 - populating the request_queue
"""


class ScraperQueueManager(StockDbBase):
    def __init__(self, use_tor=True):
        super(ScraperQueueManager, self).__init__()
        self.priority_scrapers = (MarketWatchRequestLiveScraper(), IndexLiveScraper(), FuturesScraper(), Futures1mScraper())
        self.scrapers = (RandomMarketWatchSymbols(), MarketWatchSymbolsV2(), MarketWatchHistoricalScraper())
        self.request_queue = ScraperQueue(REQUEST_QUEUE_SIZE)
        self.output_queue = Queue(maxsize=OUTPUT_QUEUE_SIZE)
        self.request_counter = Counter()
        self.successful_request_counter = Counter()
        self.failed_request_counter = Counter()
        self.event = Event()
        self.use_tor = use_tor
        self.tor_manager = Tor_Manager if use_tor else None
        self.process_pool = None

    def start(self):
        for x in range(URL_THREADS):
            args = (self.tor_manager.tor_instances[x % self.tor_manager.num_tor_instances],) if self.use_tor else ()
            t = Thread(target=self.request_thread_worker, args=args)
            t.setDaemon(True)
            t.start()
        self.log("Created {} request threads".format(URL_THREADS))

        self.pool = multiprocessing.Pool(processes=OUTPUT_PROCESSES)
        m = multiprocessing.Manager()
        process_queue = m.Queue()
        self.process_queue = process_queue
        self.process_pool = []
        for x in range(OUTPUT_PROCESSES):
            self.process_pool.append(self.pool.apply_async(output_worker_process, (process_queue, 'scraper.log')))
            a = self.pool.apply_async(output_worker_process, (process_queue, self.logger.get_file_name()))
            a.ready()
        self.log("Created {} output processes".format(OUTPUT_PROCESSES))

        self.last_process_check = datetime.min
        self.launch_queue_logger()
        try:
            while not self.event.is_set():
                allNone = True
                for scraper in self.priority_scrapers:
                    request_queue_input = scraper.get_next_input()
                    if request_queue_input:
                        allNone = False
                        request_queue_input.callback = scraper.__class__.__name__
                        self.request_queue.put(request_queue_input)

                # only move on to other scrapers if all priority scraper outputs are None
                if allNone is True:
                    for scraper in self.scrapers:
                        request_queue_input = scraper.get_next_input()
                        if request_queue_input:
                            request_queue_input.callback = scraper.__class__.__name__
                            self.request_queue.put(request_queue_input)

                if datetime.now() - timedelta(seconds=10) > self.last_process_check:
                    self.log('Checking processes')
                    for i, process in enumerate(self.process_pool):
                        try:
                            p = process.get(timeout=0.01)
                            i = 0
                        except multiprocessing.TimeoutError:
                            self.log('Process {} running'.format(i))
                            pass
                        except Exception as e:
                            self.log('Error in process', level='error')
                            self.log_exception(e)
                            self.process_pool[i] = self.pool.apply_async(output_worker_process, (process_queue, self.logger.get_file_name()))
                            self.log('Restarting Service {}'.format(i))

                    self.last_process_check = datetime.now()


                            # takes item out of thread queue puts it into process queue
                try:
                    while 1:
                        queue_item = self.output_queue.get(block=False)
                        process_queue.put(queue_item)
                except Empty:
                    pass
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
                rps = round(float(self.request_counter.get()) / (new_now - now).total_seconds(), 4)
                data = {'datetime_utc': new_now, 'request_queue_size': self.request_queue.get_size(), 'output_queue_size': self.output_queue.qsize(), 'process_queue_size': self.process_queue.qsize(),
                    'requests': self.request_counter.get(), 'successful_requests': self.successful_request_counter.get(), 'failed_requests': self.failed_request_counter.get(),
                    'requests_per_second': rps}
                Scraper_Repository.save_request_interval(data)
                self.log('Request Queue Size: {}'.format(data['request_queue_size']))
                self.log('Output Queue Size: {}'.format(data['output_queue_size']))
                self.log('Process Queue Size: {}'.format(data['process_queue_size']))
                self.log('Requests/sec: {}'.format(rps))
                self.log('Successful/Failed requests {}/{}'.format(self.successful_request_counter.get(), self.failed_request_counter.get()))
                self.request_counter.reset()
                self.successful_request_counter.reset()
                self.failed_request_counter.reset()
                now = new_now

    def request_thread_worker(self, tor_client=None):
        request_client = RequestClient(use_tor=bool(tor_client), tor_client=tor_client)
        queue_item = None
        try:
            while 1:
                queue_item = self.request_queue.get()
                if queue_item.get_http_method() == 'GET':
                    response = request_client.get(queue_item.get_url(), headers=queue_item.get_headers())
                else:
                    response = request_client.post(queue_item.get_url(), queue_item.get_body(), headers=queue_item.get_headers())
                self.request_counter.increment()
                # self.log(queue_item.get_metadata())
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
