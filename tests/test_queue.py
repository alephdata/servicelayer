from unittest import TestCase

from servicelayer.cache import get_redis, make_key
from servicelayer.queue import (
    push_task, poll_task, get_status, mark_task_finished
)


class QueueTest(TestCase):

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
        task = next(poll_task())
        assert task == (dataset, entity1, config)
        mark_task_finished(dataset)
        assert get_status(dataset) == {
            'total': 2,
            'finished': 1
        }
        task = next(poll_task())
        # Test that marking all tasks as finished, resets the dataset
        mark_task_finished(dataset)
        conn = get_redis()
        assert get_status(dataset) == {
            'total': 0,
            'finished': 0,
        }
        assert not conn.exists(make_key('ingest', 'pending', dataset))
