from servicelayer import settings


def get_redis(decode_responses=True):
    from redis import ConnectionPool, Redis
    if settings.REDIS_URL is None:
        from fakeredis import FakeRedis
        return FakeRedis(decode_responses=decode_responses)
    if not hasattr(settings, '_redis_pool'):
        settings._redis_pool = ConnectionPool.from_url(settings.REDIS_URL)
    return Redis(connection_pool=settings._redis_pool,
                 decode_responses=decode_responses)
