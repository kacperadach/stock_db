import sys
import traceback
from threading import Thread
import signal
from contextlib import contextmanager

from queue import Empty
from datetime import datetime
from time import sleep

from acquisition.scrapers import ALL_SCRAPERS

PROCESSING_TIMEOUT = 60
MAX_RETRIES = 3

def raise_timeout(signum, frame):
    raise TimeoutError

@contextmanager
def timeout(seconds):
    # Register a function to raise a TimeoutError on the signal.
    signal.signal(signal.SIGALRM, raise_timeout)
    # Schedule the signal to be sent after ``time``.
    signal.alarm(seconds)

    try:
        yield
    except TimeoutError:
        pass
    finally:
        # Unregister the signal so it won't be triggered
        # if the timeout is not reached.
        signal.signal(signal.SIGALRM, signal.SIG_IGN)

def log(log_queue, process_number, message):
    log_queue.put('{} | {} - {}\n'.format(process_number, datetime.now().strftime("%d/%m/%Y %H:%M:%S"), message))

def process(log_queue, process_number, scraper, queue_item):
    try:
        with timeout(PROCESSING_TIMEOUT + 1):
            scraper.process_data(queue_item)
    except Exception as e:
        log(log_queue, process_number, 'ERROR')
        log(log_queue, process_number,
            'Error occurred while processing data for scraper {}: {}'.format(scraper, str(e)))

def output_worker_process(process_queue, log_queue, process_number):
    log(log_queue, process_number, 'Started worker process')
    try:
        while 1:
            try:
                queue_item = process_queue.get(block=False)
            except Empty:
                sleep(0.1)
                continue
            start = datetime.utcnow()

            callback_scraper = queue_item.callback.split(".")[-1]
            scraper = None
            for s in ALL_SCRAPERS:
                if s.__name__ == callback_scraper:
                    scraper = s()
            if scraper is None:
                log(log_queue, process_number, 'Could not find scraper: {}'.format(callback_scraper))
                sys.exit(1)

            retries = 0
            while retries < MAX_RETRIES:
                t = Thread(target=process, args=(log_queue, process_number, scraper, queue_item))
                t.setDaemon(True)
                t.start()

                while t.isAlive() and (datetime.utcnow() - start).total_seconds() < PROCESSING_TIMEOUT:
                    pass

                if t.isAlive():
                    retries += 1
                    frame = sys._current_frames().get(t.ident, None)
                    if frame:
                        stack = "Processing stuck after {} seconds:\n".format(PROCESSING_TIMEOUT)
                        for filename, lineno, name, line in traceback.extract_stack(frame):
                            stack += "{} - {} - {} - {}\n".format(filename, lineno, name, line)
                        log(log_queue, process_number, "{}".format(stack))

            if retries >= MAX_RETRIES:
                raise RuntimeError("Processing stuck")

            seconds_took = (datetime.utcnow() - start).total_seconds()

            log(log_queue, process_number,
                '{} - processing took {}s: {}'.format(callback_scraper, seconds_took, queue_item.get_metadata()))
            if seconds_took > 5:
                log(log_queue, process_number,
                    'Slow output processing for metadata: {} - took {} seconds'.format(queue_item.get_metadata(),
                                                                                       seconds_took))
    except Exception:
        log(log_queue, process_number, 'ERROR')
        log(log_queue, process_number, 'Unexpected error occurred: {}'.format(traceback.format_exc()))
        sys.exit(1)


if __name__ == "__main__":
    with timeout(5):
        print('test')
        sleep(6)
        print('test6')

    # import os, signal
    #
    # def test2():
    #     sleep(5)
    #     test1()
    #
    # def test1():
    #     while 1:
    #         sleep(2)
    #         test2()
    #
    # def test():
    #     test1()
    #
    # t = Thread(target=test)
    # t.setDaemon(True)
    # t.start()
    #
    # # sleep(8)
    #
    # os.kill(11, signal.SIGSTOP)
    # i = 0
    #
    # # frame = sys._current_frames().get(t.ident, None)
    # # stack = ""
    # # for filename, lineno, name, line in traceback.extract_stack(frame):
    # #     stack += "{} - {} - {} - {}\n".format(filename, lineno, name, line)
    # # i = 0
