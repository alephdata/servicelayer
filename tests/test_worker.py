from unittest import TestCase
import pytest

from servicelayer.cache import get_fakeredis
from servicelayer.jobs import Job
from servicelayer import worker


worker.TASK_FETCH_RETRY = 1


class CountingWorker(worker.Worker):
    def boot(self):
        self.test_done = 0

    def handle(self, task):
        self.test_done += 1


class WorkerTest(TestCase):
    def test_run(self):
        conn = get_fakeredis()
        operation = "lala"
        worker = CountingWorker(conn=conn, stages=[operation])
        worker.sync()
        assert worker.test_done == 0, worker.test_done
        job = Job.create(conn, "test")
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
        assert worker.exit_code == 0, worker.exit_code
        assert worker.test_done == 1, worker.test_done
        worker.retry(task)
        worker.run(blocking=False)
        assert job.is_done()
        assert worker.exit_code == 0, worker.exit_code
        worker.num_threads = None
        worker.retry(task)
        worker.run(blocking=False)
        assert job.is_done()
        assert worker.exit_code == 0, worker.exit_code
        try:
            worker._handle_signal(5, None)
        except SystemExit as exc:
            assert exc.code == 5, exc.code
        with pytest.raises(SystemExit) as exc:  # noqa
            worker._handle_signal(5, None)
