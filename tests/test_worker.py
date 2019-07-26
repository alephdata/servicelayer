from unittest import TestCase

from servicelayer.cache import get_fakeredis
from servicelayer.jobs import Job, Stage, Task
from servicelayer.worker import Worker


class CountingWorker(Worker):

    def boot(self):
        self.test_done = 0

    def handle(self, task):
        self.test_done += 1


class WorkerTest(TestCase):

    def test_run(self):
        conn = get_fakeredis()
        operation = 'lala'
        worker = CountingWorker(conn=conn, stages=[operation])
        worker.sync()
        assert worker.test_done == 0, worker.test_done
        job = Job.create(conn, 'test')
        stage = job.get_stage(operation)
        task = stage.queue({}, {})
        assert not job.is_done()
        assert worker.test_done == 0, worker.test_done
        worker.sync()
        assert worker.test_done == 1, worker.test_done
        assert job.is_done()
        worker.retry(task)
        assert not job.is_done()
        worker.sync()
        assert job.is_done()
        assert worker.test_done == 1, worker.test_done
        worker.shutdown()
        worker.retry(task)
        worker.process()
