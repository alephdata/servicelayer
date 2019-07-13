from unittest import TestCase

from servicelayer.cache import get_fakeredis
from servicelayer.jobs import JobStage
from servicelayer.worker import Worker


class CountingWorker(Worker):

    def boot(self):
        self.test_done = 0

    def handle(self, stage, payload, context):
        self.test_done += 1


class WorkerTest(TestCase):

    def test_run(self):
        conn = get_fakeredis()
        operation = 'lala'
        worker = CountingWorker(conn=conn, stages=[operation])
        worker.sync()
        assert worker.test_done == 0, worker.test_done
        stage = JobStage(conn, operation, 'job_id', 'test')
        stage.queue_task({}, {})
        assert not stage.is_done()
        assert worker.test_done == 0, worker.test_done
        worker.sync()
        assert worker.test_done == 1, worker.test_done
        assert stage.is_done()
        worker.retry(stage, {}, {})
        assert not stage.is_done()
        worker.sync()
        assert stage.is_done()
        assert worker.test_done == 1, worker.test_done
        worker.shutdown()
        worker.retry(stage, {}, {})
        worker.process()
