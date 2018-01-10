from Queue import Queue

from core.QueueItem import QueueItem

class RequestQueue():

    def __init__(self, queue_size):
        self.request_queue = Queue(maxsize=queue_size)

    def put(self, obj):
        if not isinstance(obj, QueueItem):
            raise RequestQueueException("Queue input was not of the type QueueItem")
        self.request_queue.put(obj)

    def get(self):
        return self.request_queue.get(block=True)

class RequestQueueException(Exception):
    pass
