from unittest import TestCase

from servicelayer.cache import get_fakeredis
from servicelayer.queue import ServiceQueue, RateLimit, Progress


class QueueTest(TestCase):

    def test_service_queue(self):
        conn = get_fakeredis()
        ds = 'test_1'
        queue = ServiceQueue(conn, ServiceQueue.OP_INGEST, ds)
        status = queue.progress.get()
        assert status['pending'] == 0
        assert status['finished'] == 0
        queue.queue_task({'test': 'foo'}, {})
        status = queue.progress.get()
        assert status['pending'] == 1
        assert status['finished'] == 0
        task = ServiceQueue.get_operation_task(conn, ServiceQueue.OP_INGEST)
        nq, payload, context = task
        assert nq.dataset == queue.dataset
        assert payload['test'] == 'foo'
        status = queue.progress.get()
        assert status['pending'] == 1
        assert status['finished'] == 0
        queue.task_done()
        status = queue.progress.get()
        assert status['pending'] == 0
        assert status['finished'] == 1
        task = ServiceQueue.get_operation_task(conn, ServiceQueue.OP_INGEST,
                                               timeout=1)
        nq, payload, context = task
        assert payload is None
        queue.remove()
        status = queue.progress.get()
        assert status['pending'] == 0
        assert status['finished'] == 0

    def test_queue_clear(self):
        conn = get_fakeredis()
        ds = 'test_1'
        queue = ServiceQueue(conn, ServiceQueue.OP_INGEST, ds)
        queue.queue_task({'test': 'foo'}, {})
        status = queue.progress.get()
        assert status['pending'] == 1
        ServiceQueue.remove_dataset(conn, ds)
        status = queue.progress.get()
        assert status['pending'] == 0

    def test_progress(self):
        conn = get_fakeredis()
        ds = 'test_2'
        progress = Progress(conn, ServiceQueue.OP_INGEST, ds)
        status = progress.get()
        assert status['pending'] == 0
        assert status['finished'] == 0
        progress.mark_pending()
        status = progress.get()
        assert status['pending'] == 1
        assert status['finished'] == 0
        full = Progress.get_dataset_status(conn, ds)
        assert full['pending'] == 1
        progress.mark_finished()
        status = progress.get()
        assert status['pending'] == 0
        assert status['finished'] == 1
        progress.remove()
        status = progress.get()
        assert status['finished'] == 0

    def test_rate(self):
        conn = get_fakeredis()
        limit = RateLimit(conn, 'banana', limit=10)
        assert limit.check()
        limit.update()
        assert limit.check()
        for i in range(13):
            limit.update()
        assert not limit.check()
