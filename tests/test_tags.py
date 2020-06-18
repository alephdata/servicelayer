from unittest import TestCase
from datetime import datetime

from servicelayer.tags import Tags


class TagsTest(TestCase):

    def test_redis(self):
        tags = Tags('test_tags')
        assert 'test_tags' in repr(tags)
        key = 'banana'
        assert not tags.exists(key)
        assert not tags.get(key)
        tags.set(key, 'split')
        assert tags.exists(key)
        assert tags.get(key) == 'split', tags.get(key)
        tags.set(key, 'new')
        assert tags.get(key) == 'new', tags.get(key)
        now = datetime.utcnow()
        assert not tags.get(key, since=now)
        assert not tags.exists(key, since=now)
        tags.delete(key=key)
        assert not tags.get(key)
        assert not tags.exists(key)
        tags.set(key, 'split')
        tags.delete()
        assert not tags.get(key)
        tags.set(key, 'split')
        tags.delete(prefix='ba')
        assert not tags.get(key)
        tags.close()
