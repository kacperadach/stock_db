import sys
import traceback
from threading import Thread

from Queue import Empty
from datetime import datetime
from time import sleep

from acquisition.scrapers import ALL_SCRAPERS


def log(process_number, message):
    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    print '{} | {} - {}'.format(process_number, now, message)

def process(process_number, scraper, queue_item):
    try:
        scraper.process_data(queue_item)
    except Exception as e:
        log(process_number, 'ERROR')
        log(process_number, 'Error occurred while processing data for scraper {}: {}'.format(scraper, str(e)))

def output_worker_process(process_queue, processing_file_path, process_number):
    sys.stdout = open(processing_file_path, "a", buffering=0)

    log(process_number, 'Started worker process')
    try:
        while 1:
            try:
                queue_item = process_queue.get(block=False)
            except Empty:
                sleep(0.1)
                continue
            log(process_number, 'Got from queue: {}'.format(queue_item.get_metadata()))
            start = datetime.utcnow()

            callback_scraper = queue_item.callback.split(".")[-1]
            scraper = None
            for s in ALL_SCRAPERS:
                if s.__name__ == callback_scraper:
                    scraper = s()
            if scraper is None:
                log(process_number, 'Could not find scraper: {}'.format(callback_scraper))
                sys.exit(1)

            t = Thread(target=process, args=(process_number, scraper, queue_item))
            t.setDaemon(True)
            t.start()

            while t.isAlive() and (datetime.utcnow() - start).total_seconds() < 20:
                pass

            if t.isAlive():
                frame = sys._current_frames().get(t.ident, None)
                if frame:
                    log(process_number, "{}\n{}\n{}".format(frame.f_code.co_filename, frame.f_code.co_name, frame.f_code.co_firstlineno))
                    log(process_number, "{}".format(traceback.extract_stack(frame)))
                raise RuntimeError("Processing stuck")

            seconds_took = (datetime.utcnow() - start).total_seconds()

            log(process_number, '{} - processing took {}s: {}'.format(callback_scraper, seconds_took, queue_item.get_metadata()))
            if seconds_took > 5:
                log(process_number, 'Slow output processing for metadata: {} - took {} seconds'.format(queue_item.get_metadata(), seconds_took))
    except Exception as e:
        log(process_number, 'ERROR')
        log(process_number, 'Unexpected error occurred: {}'.format(traceback.format_exc(e)))
        sys.exit(1)


if __name__ == "__main__":
    log(1, 'test')
