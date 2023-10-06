import logging
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


class NoOpWorker(worker.Worker):
    def handle(self, task):
        pass


def test_run():
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


def test_fails(caplog):
    caplog.set_level(logging.DEBUG)
    conn = get_fakeredis()
    operation = "fails"
    job = Job.create(conn, "test")
    stage = job.get_stage(operation)
    task = stage.queue({}, {})

    worker = NoOpWorker(conn=conn, stages=[operation])
    worker.sync()
    for _ in range(4):
        worker.retry(task)

    log_messages = [r.msg for r in caplog.records]
    assert "Queueing failed task for retry #1/3..." in log_messages
    assert "Queueing failed task for retry #2/3..." in log_messages
    assert "Queueing failed task for retry #3/3..." in log_messages
    assert "Failed task, exhausted retry count of 3" in log_messages
