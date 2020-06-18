from unittest import TestCase

from servicelayer.tags import Tags


class TagsTest(TestCase):

    def test_redis(self):
        tags = Tags('test_tags')
        assert 'test_tags' in repr(tags)
        key = 'banana'
        assert not tags.get(key)
        tags.set(key, 'split')
        assert tags.get(key) == 'split', tags.get(key)
        tags.set(key, 'new')
        assert tags.get(key) == 'new', tags.get(key)
        tags.delete(key)
        assert not tags.get(key)
        tags.close()
