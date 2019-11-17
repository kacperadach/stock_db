from queue import Queue

from core.QueueItem import QueueItem

class ScraperQueue():

    def __init__(self, queue_size):
        self.queue = Queue(maxsize=queue_size)

    def put(self, obj):
        if not isinstance(obj, QueueItem):
            raise ScraperQueueException("Queue input was not of the type QueueItem")
        self.queue.put(obj)

    def get(self):
        return self.queue.get(block=True)

    def get_size(self):
        return self.queue.qsize()

class ScraperQueueException(Exception):
    pass
