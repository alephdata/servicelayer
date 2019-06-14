import time
import random
import logging

log = logging.getLogger(__name__)


def backoff(failures=0):
    """Implement a random, growing delay between external service retries."""
    sleep = max(1, failures - 1) + random.random()
    log.debug("Back-off: %.2fs", sleep)
    time.sleep(sleep)


def service_retries():
    """A default number of tries to re-try an external service."""
    return range(30)
