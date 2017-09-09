from Queue import Queue, Empty, Full
from datetime import datetime
from threading import Thread

from bs4 import BeautifulSoup, SoupStrainer

from db.Finance import Finance_DB
from logger import Logger
from request.request_queue import RequestQueue

CURRENT_OWNERSHIP = "https://fintel.io/so/us/{}?page={}"
INSIDER_TRANSACTIONS = "https://fintel.io/n/us/{}?page={}"

OUTPUT_QUEUE_SIZE = 5000
URL_QUEUE_SIZE = 10000
QUEUE_TIMEOUT = 30

INSIDER_COLLECTION = 'fintel_insider'
OWNERSHIP_COLLECTION = 'fintel_ownership'

class FintelAcquisitionError(Exception):
    pass

class FintelAcquisition():

    def __init__(self, num_request_threads=10, num_parsing_threads=5):
        self.task_name = "FintelAcquisition"
        self.num_request_threads = num_request_threads
        self.num_parsing_threads = num_parsing_threads
        self.finance_db = Finance_DB
        self.request_queue = RequestQueue(num_request_threads=self.num_request_threads)
        self.url_queue = Queue(maxsize=URL_QUEUE_SIZE)
        self.output_queue = Queue(maxsize=OUTPUT_QUEUE_SIZE)
        self.documents = []

    def _log(self, msg, level='info'):
        Logger.log(msg, level=level, threadname=self.task_name)

    def _insider_worker(self, data):
        response = data['data']
        parsed_response = self._parse_response(response)
        if len(parsed_response) > 0:
            if self.finance_db.collection != INSIDER_COLLECTION:
                self.finance_db.set_collection(INSIDER_COLLECTION)
            documents = []
            for document in parsed_response:
                document['symbol'] = data['symbol']
                if len(list(self.finance_db.find(document).limit(1))) == 0:
                    documents.append(document)
            if documents:
                self.finance_db.insert_many(documents)
            if not self.first_page_only and len(parsed_response) >= 50:
                data['page'] += 1
                data['url'] = data['url'].split('=')[0] + "={}".format(data['page'])
                data['data'] = None
                print 'adding ' + data['url']
                self.url_queue.put(data, timeout=QUEUE_TIMEOUT)

    def _ownership_worker(self, data):
        print 'working: {}'.format(data['symbol'])
        response = data['data']
        parsed_response = self._parse_response(response)
        if len(parsed_response) > 0:
            if self.finance_db.collection != OWNERSHIP_COLLECTION:
                self.finance_db.set_collection(OWNERSHIP_COLLECTION)
            documents = []
            for document in parsed_response:
                document['symbol'] = data['symbol']
                if len(list(self.finance_db.find(document).limit(1))) == 0:
                    documents.append(document)
            if documents:
                self.finance_db.insert_many(documents)

    def start_input_worker(self, symbols, base_url):
        def _input_worker():
            try:
                for symbol in symbols:
                    self.url_queue.put({"symbol": symbol, "page": 1, "url": base_url.format(symbol, 1)}, timeout=QUEUE_TIMEOUT)
                print 'Input worker finished'
            except Full:
                self._log('Queue timeout during input execution')

        t = Thread(target=_input_worker)
        t.setDaemon(True)
        t.start()
        return t

    def start_output_worker(self, output_worker):
        def _output_worker():
            while self.url_queue.qsize() + self.output_queue.qsize() > 0:
                try:
                    data = self.output_queue.get(timeout=QUEUE_TIMEOUT)
                    output_worker(data)
                except (Empty, Full):
                    pass

        threads = []
        for i in range(self.num_parsing_threads):
            t = Thread(target=_output_worker)
            t.setDaemon(True)
            t.start()
            threads.append(t)
        return threads

    def execute_insider(self, symbols, first_page_only=False):
        self.first_page_only = first_page_only
        self._execute(symbols, 'insider')

    def execute_ownership(self, symbols):
        self._execute(symbols, 'ownership')

    def _execute(self, symbols, mode):
        start = datetime.now()
        if mode.lower() == 'insider':
            base_url = INSIDER_TRANSACTIONS
            output_worker = self._insider_worker
        elif mode.lower() == 'ownership':
            base_url = CURRENT_OWNERSHIP
            output_worker = self._ownership_worker
        else:
            raise FintelAcquisitionError('Invalid mode: {}'.format(mode))

        self.request_queue.start(self.url_queue, self.output_queue)

        input = self.start_input_worker(symbols, base_url)
        output_threads = self.start_output_worker(output_worker)

        for t in output_threads:
            t.join()
        input.join()
        self._log("Finished Execution, time taken: {}".format(datetime.now()-start))

    def _parse_response(self, response):
        bs = BeautifulSoup(response, 'lxml', parse_only=SoupStrainer('table', {'id': 'transactions'}))
        rows = bs.findAll('tr')
        if len(rows) <= 1:
            return []

        parsed_data = []
        table_headers = None
        for row in rows:
            if table_headers is None:
                table_headers = map(lambda x: x.text, row.findChildren('th'))
            else:
                table_data = map(lambda x: x.text, row.findChildren('td'))
                data = {}
                for i, header in enumerate(table_headers):
                    if header:
                        data[header] = table_data[i]
                parsed_data.append(data)
        return parsed_data


if __name__ == "__main__":
    from acquisition.symbol.financial_symbols import Financial_Symbols
    FintelAcquisition().execute_ownership(Financial_Symbols.get_all()[0:100])
    #FintelAcquisition().execute_insider(['AAPL'])