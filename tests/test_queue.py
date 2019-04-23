from unittest import TestCase

from servicelayer.cache import get_redis, make_key
from servicelayer.queue import (
    push_task, poll_task, get_status, mark_task_finished,
    TASK_TOTAL, TASK_FINISHED, INGESTION_FINISHED,
)
from servicelayer.settings import QUEUE_HIGH, QUEUE_LOW


class QueueTest(TestCase):

    def test_task_queue(self):
        dataset1 = 'us-fake-1'
        dataset2 = 'us-fake-2'
        entity1 = {
            'id': 'fake-entity-1',
        }
        entity2 = {
            'id': 'fake-entity-2',
        }
        config = {
            'ocr_langs': ['en', 'fr']
        }
        push_task(QUEUE_LOW, dataset1, entity1, config)
        push_task(QUEUE_HIGH, dataset1, entity2, config)
        push_task(QUEUE_LOW, dataset2, entity1, config)
        conn = get_redis()
        assert conn.sismember(
            make_key('ingest', 'queues', QUEUE_HIGH), dataset1
        )
        assert not conn.sismember(
            make_key('ingest', 'queues', QUEUE_LOW), dataset1
        )
        assert conn.sismember(
            make_key('ingest', 'queues', QUEUE_LOW), dataset2
        )

        assert get_status(dataset1) == {
            TASK_TOTAL: 2,
            TASK_FINISHED: 0,
            INGESTION_FINISHED: False,
        }
        assert get_status(dataset2) == {
            TASK_TOTAL: 1,
            TASK_FINISHED: 0,
            INGESTION_FINISHED: False,
        }
        task = next(poll_task())
        assert (task == (dataset1, entity1, config) or
                task == (dataset1, entity2, config))
        mark_task_finished(dataset1)
        assert get_status(dataset1) == {
            TASK_TOTAL: 2,
            TASK_FINISHED: 1,
            INGESTION_FINISHED: False,
        }
        task = next(poll_task())
        assert (task == (dataset1, entity1, config) or
                task == (dataset1, entity2, config))
        # Test that marking all tasks as finished, resets the dataset
        mark_task_finished(dataset1)
        assert get_status(dataset1) == {
            TASK_TOTAL: 0,
            TASK_FINISHED: 0,
            INGESTION_FINISHED: True,
        }
        assert not conn.exists(make_key('ingest', 'pending', dataset1))
        assert conn.sismember(
            make_key('ingest', 'queues', 'finished'), dataset1
        )
        task = next(poll_task())
        assert task == (dataset2, entity1, config)
