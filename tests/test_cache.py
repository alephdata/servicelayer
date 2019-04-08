from uuid import uuid4
from unittest import TestCase

from servicelayer.cache import (
    get_redis, make_key, push_task, poll_task, get_status, mark_task_finished
)


class CacheTest(TestCase):

    def test_redis(self):
        key = make_key('test', uuid4())
        conn = get_redis()
        assert not conn.exists(key)
        conn.set(key, 'banana')
        assert conn.get(key) == 'banana', conn.get(key)
        assert conn.exists(key)

    def test_task_queue(self):
        dataset = 'us-fake'
        entity1 = {
            'id': 'fake-entity-1',
        }
        entity2 = {
            'id': 'fake-entity-2',
        }
        config = {
            'ocr_langs': ['en', 'fr']
        }
        push_task('QUEUE_HIGH', dataset, entity1, config)
        push_task('QUEUE_LOW', dataset, entity2, config)
        assert get_status(dataset) == {
            'total': 2,
            'finished': 0,
        }
        # weird work-around because `yield from` doesn't work inside pytest
        # pytest just skips the test without any warning
        for task in poll_task():
            assert task == (dataset, entity1, config)
            mark_task_finished(dataset)
            assert get_status(dataset) == {
                'total': 2,
                'finished': 1
            }
            break
        for task in poll_task():
            # Test that marking all tasks as finished, resets the dataset
            mark_task_finished(dataset)
            conn = get_redis()
            assert get_status(dataset) == {
                'total': 0,
                'finished': 0,
            }
            assert not conn.exists(make_key('ingest', 'pending', dataset))
            break
