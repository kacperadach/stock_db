from multiprocessing import RawValue, Lock

class Counter(object):

    def __init__(self):
        self.val = RawValue('i', 0)
        self.lock = Lock()

    def increment(self):
        with self.lock:
            self.val.value += 1

    def get(self):
        with self.lock:
            return self.val.value

    def reset(self):
        with self.lock:
            self.val.value = 0
