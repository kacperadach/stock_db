import sys
import traceback

from Queue import Empty
from datetime import datetime
from time import sleep

from acquisition.scrapers import ALL_SCRAPERS

def output_worker_process(process_queue, processing_file_path):
    sys.stdout = open(processing_file_path, "a", buffering=0)

    print 'Started worker process'
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
                error = 'could not find scraper: {}'.format(callback_scraper)
                print error
                raise RuntimeError(error)
            try:
                scraper.process_data(queue_item)
            except Exception as e:
                print 'Error occurred while processing data for scraper {}: {}'.format(callback_scraper, str(e))

            seconds_took = (datetime.utcnow() - start).total_seconds()
            print '{} - processing took {}s: {}'.format(callback_scraper, seconds_took, queue_item.get_metadata())

            if seconds_took > 5:
                print 'Slow output processing for metadata: {} - took {} seconds'.format(queue_item.get_metadata(), seconds_took)
    except Exception as e:
        error = 'Unexpected error occurred: {}'.format(traceback.format_exc(e))
        raise RuntimeError(error)

if __name__ == "__main__":
    from acquisition.scrapers import ALL_SCRAPERS
    output_worker_process(None, 'test')
