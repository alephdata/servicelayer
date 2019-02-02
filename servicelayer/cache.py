from servicelayer import settings


def get_fakeredis(decode_responses=True):
    from fakeredis import FakeRedis
    if not hasattr(settings, '_redis_fake'):
        settings._redis_fake = FakeRedis(decode_responses=decode_responses)
    return settings._redis_fake


def get_redis(decode_responses=True):
    from redis import ConnectionPool, Redis
    if settings.REDIS_URL is None:
        return get_fakeredis(decode_responses=True)
    if not hasattr(settings, '_redis_pool'):
        settings._redis_pool = ConnectionPool.from_url(settings.REDIS_URL)
    return Redis(connection_pool=settings._redis_pool,
                 decode_responses=decode_responses)
