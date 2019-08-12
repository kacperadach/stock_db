import traceback
from datetime import datetime
import os

from logger import AppLogger

from acquisition.scrapers import ALL_SCRAPERS

def output_worker_process(process_queue, log_file_name):
    logger = AppLogger(output_process=True, file_name=log_file_name)
    logger.log('Started Output Process, pid: {}'.format(os.getpid()))
    try :
        while 1:
            queue_item = process_queue.get(block=True)
            start = datetime.utcnow()

            callback_scraper = queue_item.callback.split(".")[-1]
            scraper = None
            for s in ALL_SCRAPERS:
                if s.__name__ == callback_scraper:
                    scraper = s()
            if scraper is None:
                raise RuntimeError('could not find scraper: {}'.format(callback_scraper))

            logger.log('processing')
            scraper.process_data(queue_item)
            seconds_took = (datetime.utcnow() - start).total_seconds()
            logger.log('Output processing: - took {} seconds'.format(seconds_took))

            # if seconds_took > 5:
            # logger.log('Slow output processing for metadata: {} - took {} seconds'.format(queue_item.get_metadata(), seconds_took))
    except Exception as e:
        logger.log('ERROR ERROR ERROR: {}'.format(e), level='ERROR')
        logger.log(traceback.format_exc(e))


if __name__ == "__main__":
    from acquisition.scrapers import ALL_SCRAPERS
    i = 0