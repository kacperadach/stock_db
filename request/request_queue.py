from Queue import Queue, Empty
from threading import Thread

from request.base.request_client import RequestClient
from request.base.tor_client import TorClient
from app.config import App_Config
from logger.queue_logger import QueueLogger

class RequestQueueException(Exception):
    pass

class RequestQueue():

    def __init__(self, num_request_threads=10, num_retries=2):
        self.num_request_threads = num_request_threads
        self.num_retries = num_retries
        self.url_queue = None
        self.output_queue = None
        self.request_threads = []
        self.use_tor = App_Config.use_tor
        self.tor_client = TorClient()
        self.request_client = RequestClient()
        self.queue_logger = QueueLogger()

    def start(self, url_queue, output_queue):
        def execute_worker():
            if self.use_tor:
                self.tor_client.connect()

            self.queue_logger.start_logging({"url_queue": url_queue, "output_queue": output_queue})

            self.request_threads = []
            for i in range(self.num_request_threads):
                worker = Thread(target=self.request_worker, args=(url_queue, output_queue,))
                worker.setDaemon(True)
                self.request_threads.append(worker)
                worker.start()

            for thread in self.request_threads:
                thread.join()

            self.queue_logger.stop_logging()

            if self.use_tor:
                self.tor_client.disconnect()
            print 'Finished Request Queue Worker'

        t = Thread(target=execute_worker)
        t.setDaemon(True)
        t.start()

    def request_worker(self, url_queue, output_queue):
        try:
            while True:
                url_obj = url_queue.get()
                url = url_obj['url']
                retry = 0
                response = None
                while response is None or (response.status_code != 200 and retry < self.num_retries):
                    if retry > 0 and self.use_tor:
                        self.tor_client.new_nym()
                    response = self.request_client.get(url)
                    retry += 1
                url_obj['data'] = response.get_data()
                output_queue.put(url_obj)
        except Exception as e:
            raise RequestQueueException(str(e))

if __name__ == "__main__":
    from acquisition.symbol.financial_symbols import Financial_Symbols
    CURRENT_OWNERSHIP = "https://fintel.io/so/us/{}"
    rq = RequestQueue()
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

