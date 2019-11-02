import logging
import traceback
from Queue import Empty
from datetime import datetime
import os
from time import sleep

from logger import AppLogger

from acquisition.scrapers import ALL_SCRAPERS

def output_worker_process(process_queue, log_file_name):
    # logger = AppLogger()
    # logger.log('Started Output Process, pid: {}'.format(os.getpid()))
    print 'Started worker process'
    try:
        while 1:
            try:
                print 'fetching from process queue'
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
                e = RuntimeError('could not find scraper: {}'.format(callback_scraper))
                # logger.log(traceback.format_exc(e))
                print 'could not find scraper'
                raise e
            try:
                scraper.process_data(queue_item)
            except Exception as e:
                print 'error occurred while processing data'
                # logger.log('PROCESSING ERROR FATAL: {} - {}'.format(scraper.__name__, e))

            seconds_took = (datetime.utcnow() - start).total_seconds()
            # logger.log('Output processing: - took {} seconds: {}'.format(seconds_took, queue_item.get_metadata()))

            # if seconds_took > 5:
            # logger.log('Slow output processing for metadata: {} - took {} seconds'.format(queue_item.get_metadata(), seconds_took))
    except Exception as e:
        # logger.log('ERROR ERROR ERROR: {}'.format(e), level='error')
        # logger.log(traceback.format_exc(e))
        print 'unexpected error occurred'
        raise e

if __name__ == "__main__":
    from acquisition.scrapers import ALL_SCRAPERS
    output_worker_process(None, 'test')
