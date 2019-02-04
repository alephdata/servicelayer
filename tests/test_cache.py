from uuid import uuid4
from unittest import TestCase

from servicelayer.cache import get_redis, make_key


class CacheTest(TestCase):

    def test_redis(self):
        key = make_key('test', uuid4())
        conn = get_redis()
        assert not conn.exists(key)
        conn.set(key, 'banana')
        assert conn.get(key) == 'banana', conn.get(key)
        assert conn.exists(key)
