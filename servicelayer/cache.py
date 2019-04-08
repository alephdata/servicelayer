from fakeredis import FakeRedis
from redis import ConnectionPool, Redis

from servicelayer import settings


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
    return Redis(connection_pool=get_redis_pool(), decode_responses=True)


def make_key(*criteria):
    """Make a string key out of many criteria."""
    criteria = [c or '' for c in criteria]
    criteria = [str(c) for c in criteria]
    return ':'.join(criteria)
