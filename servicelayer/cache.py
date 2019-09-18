import logging
from fakeredis import FakeRedis
from redis import ConnectionPool, Redis, BusyLoadingError

from servicelayer import settings
from servicelayer.util import service_retries, backoff

log = logging.getLogger(__name__)


def get_fakeredis():
    if not hasattr(settings, '_redis_fake'):
        settings._redis_fake = FakeRedis(decode_responses=True)
    return settings._redis_fake


def get_redis_pool():
    if not hasattr(settings, '_redis_pool'):
        settings._redis_pool = ConnectionPool.from_url(settings.REDIS_URL,
                                                       decode_responses=True)
    return settings._redis_pool


def get_redis():
    """Create a redis connection."""
    if settings.REDIS_URL is None:
        return get_fakeredis()
    conn = Redis(connection_pool=get_redis_pool(), decode_responses=True)
    wait_for_redis(conn)
    return conn


def wait_for_redis(conn):
    """Wait for redis to load its data into memory on initial system
    bootup."""
    for attempt in service_retries():
        try:
            conn.get('test_redis_ready')
            return conn
        except BusyLoadingError:
            log.info("Waiting for redis to load...")
            backoff(failures=attempt)
    raise RuntimeError("Redis is not ready.")


def make_key(*criteria):
    """Make a string key out of many criteria."""
    parts = []
    for criterion in criteria:
        if criterion is None:
            continue
        criterion = str(criterion)
        criterion = criterion.replace(':', '#')
        parts.append(criterion)
    return ':'.join(parts)
