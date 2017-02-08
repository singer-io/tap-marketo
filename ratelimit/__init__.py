from threading import Semaphore, Timer
from functools import wraps

## based on http://stackoverflow.com/a/30918773

def ratelimit(limit, every):
    def limitdecorator(fn):
        semaphore = Semaphore(limit)
        @wraps(fn)
        def wrapper(*args, **kwargs):
            semaphore.acquire()
            try:
                result = fn(*args, **kwargs)
            finally:
                timer = Timer(every, semaphore.release)
                timer.setDaemon(True) # allows the timer to be canceled on exit
                timer.start()
                return result
        return wrapper
    return limitdecorator
