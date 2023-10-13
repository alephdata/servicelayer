import logging
import pytest
from prometheus_client import REGISTRY
from prometheus_client.metrics import MetricWrapperBase

from servicelayer.cache import get_fakeredis
from servicelayer.jobs import Job
from servicelayer import worker


worker.TASK_FETCH_RETRY = 1


class CountingWorker(worker.Worker):
    def boot(self):
        self.test_done = 0

    def handle(self, task):
        self.test_done += 1


class FailingWorker(worker.Worker):
    def handle(self, task):
        raise Exception("Woops")


class NoOpWorker(worker.Worker):
    def handle(self, task):
        pass


class PrometheusTests:
    def setup_method(self, method):
        # This relies on internal implementation details of the client to reset
        # previously collected metrics before every test execution. Unfortunately,
        # there is no clean way of achieving the same thing that doesn't add a lot
        # of complexity to the test and application code.
        collectors = REGISTRY._collector_to_names.keys()
        for collector in collectors:
            if isinstance(collector, MetricWrapperBase):
                collector._metrics.clear()
                collector._metric_init()

    def test_prometheus_succeeded(self):
        conn = get_fakeredis()
        worker = CountingWorker(conn=conn, stages=["ingest"])
        job = Job.create(conn, "test")
        stage = job.get_stage("ingest")
        stage.queue({}, {})
        worker.sync()

        labels = {"stage": "ingest"}
        success_labels = {"stage": "ingest", "retries": "0"}

        started = REGISTRY.get_sample_value("task_started_total", labels)
        succeeded = REGISTRY.get_sample_value("task_succeeded_total", success_labels)

        # Under the hood, histogram metrics create multiple time series tracking
        # the number and sum of observations, as well as individual histogram buckets.
        duration_sum = REGISTRY.get_sample_value("task_duration_seconds_sum", labels)
        duration_count = REGISTRY.get_sample_value(
            "task_duration_seconds_count",
            labels,
        )

        assert started == 1
        assert succeeded == 1
        assert duration_sum > 0
        assert duration_count == 1

    def test_prometheus_failed(self):
        conn = get_fakeredis()
        worker = FailingWorker(conn=conn, stages=["ingest"])
        job = Job.create(conn, "test")
        stage = job.get_stage("ingest")
        stage.queue({}, {})
        labels = {"stage": "ingest"}

        worker.sync()

        assert REGISTRY.get_sample_value("task_started_total", labels) == 1
        assert REGISTRY.get_sample_value(
            "task_failed_total",
            {
                "stage": "ingest",
                "retries": "0",
                "failed_permanently": "False",
            },
        )

        worker.sync()

        assert REGISTRY.get_sample_value("task_started_total", labels) == 2
        assert REGISTRY.get_sample_value(
            "task_failed_total",
            {
                "stage": "ingest",
                "retries": "1",
                "failed_permanently": "False",
            },
        )

        worker.sync()

        assert REGISTRY.get_sample_value("task_started_total", labels) == 3
        assert REGISTRY.get_sample_value(
            "task_failed_total",
            {
                "stage": "ingest",
                "retries": "2",
                "failed_permanently": "False",
            },
        )

        worker.sync()

        assert REGISTRY.get_sample_value("task_started_total", labels) == 4
        assert REGISTRY.get_sample_value(
            "task_failed_total",
            {
                "stage": "ingest",
                "retries": "3",
                "failed_permanently": "True",
            },
        )


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
