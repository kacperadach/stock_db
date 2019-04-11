from datetime import datetime

from logger import Logger

def timed(func):
    def timed_wrapper(*original_args, **original_kwargs):
        before = datetime.now()
        func(*original_args, **original_kwargs)
        Logger.log('Timed {}.{}: {}'.format(func.__module__, func.__name__, str(datetime.now() - before)))
    return timed_wrapper