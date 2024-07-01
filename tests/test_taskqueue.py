import datetime
from unittest import TestCase
from unittest.mock import patch
import json
from random import randrange
import time_machine

import pika
from prometheus_client import REGISTRY
from prometheus_client.metrics import MetricWrapperBase
import pytest

from servicelayer import settings
from servicelayer.cache import get_fakeredis
from servicelayer.taskqueue import (
    Worker,
    Dataset,
    Task,
    get_rabbitmq_channel,
    dataset_from_collection_id,
    declare_rabbitmq_queue,
    flush_queues,
)
from servicelayer.util import unpack_datetime

from servicelayer.taskqueue import get_priority, get_task_count, queue_task


class CountingWorker(Worker):
    def dispatch_task(self, task: Task) -> Task:
        assert isinstance(task, Task), task
        if not hasattr(self, "test_done"):
            self.test_done = 0
        self.test_done += 1
        self.test_task = task
        return task


class FailingWorker(Worker):
    def dispatch_task(self, task: Task) -> Task:
        raise Exception("Woops")


class TaskQueueTest(TestCase):
    def test_task_queue(self):
        test_queue_name = "sls-queue-ingest"
        conn = get_fakeredis()
        conn.flushdb()
        collection_id = 2
        task_id = "test-task"
        priority = randrange(1, settings.RABBITMQ_MAX_PRIORITY + 1)
        body = {
            "collection_id": 2,
            "job_id": "test-job",
            "task_id": "test-task",
            "operation": "test-op",
            "context": {},
            "payload": {},
            "priority": priority,
        }
        channel = get_rabbitmq_channel()
        declare_rabbitmq_queue(channel, test_queue_name)
        channel.queue_purge(test_queue_name)
        channel.basic_publish(
            properties=pika.BasicProperties(priority=priority),
            exchange="",
            routing_key=test_queue_name,
            body=json.dumps(body),
        )
        dataset = Dataset(conn=conn, name=dataset_from_collection_id(collection_id))
        dataset.add_task(task_id, "test-op")
        channel.close()
        status = dataset.get_status()
        assert status["finished"] == 0, status
        assert status["pending"] == 1, status
        assert status["running"] == 0, status
        started = unpack_datetime(status["start_time"])
        last_updated = unpack_datetime(status["last_update"])
        assert started < last_updated
        assert abs(started - last_updated) < datetime.timedelta(seconds=1)

        worker = CountingWorker(queues=[test_queue_name], conn=conn, num_threads=1)
        worker.process(blocking=False)

        status = dataset.get_status()
        assert status["finished"] == 0, status
        assert status["pending"] == 0, status
        assert status["running"] == 1, status
        assert worker.test_done == 1
        task = Task(**body, delivery_tag=None)
        assert task.get_retry_count(conn) == 1

        with patch("servicelayer.settings.WORKER_RETRY", 0):
            channel = get_rabbitmq_channel()
            channel.queue_purge(test_queue_name)
            channel.basic_publish(
                properties=pika.BasicProperties(priority=priority),
                exchange="",
                routing_key=test_queue_name,
                body=json.dumps(body),
            )
            dataset = Dataset(conn=conn, name=dataset_from_collection_id(collection_id))
            dataset.add_task(task_id, "test-op")
            channel.close()
            with self.assertLogs(level="ERROR") as ctx:
                with patch.object(
                    pika.channel.Channel,
                    attribute="basic_nack",
                    return_value=None,
                ) as nack_fn:
                    worker.process(blocking=False)
                    nack_fn.assert_any_call(
                        delivery_tag=1, multiple=False, requeue=False
                    )
            assert "Max retries reached for task test-task. Aborting." in ctx.output[0]
            # Assert that retry count stays the same
            assert task.get_retry_count(conn) == 1

        worker.ack_message(worker.test_task, channel)
        status = dataset.get_status()
        assert status["finished"] == 0, status
        assert status["pending"] == 0, status
        assert status["running"] == 0, status
        assert status["start_time"] is None
        assert status["last_update"] is None

    @patch("servicelayer.taskqueue.Dataset.should_execute")
    def test_task_that_shouldnt_execute(self, mock_should_execute):
        test_queue_name = "sls-queue-ingest"
        conn = get_fakeredis()
        conn.flushdb()
        collection_id = 2
        task_id = "test-task"
        priority = randrange(1, settings.RABBITMQ_MAX_PRIORITY + 1)
        body = {
            "task_id": "test-task",
            "job_id": "test-job",
            "delivery_tag": 0,
            "operation": "test-op",
            "context": {},
            "payload": {},
            "priority": priority,
            "collection_id": 2,
        }

        channel = get_rabbitmq_channel()
        declare_rabbitmq_queue(channel, test_queue_name)
        channel.queue_purge(test_queue_name)
        channel.basic_publish(
            properties=pika.BasicProperties(priority=priority),
            exchange="",
            routing_key=test_queue_name,
            body=json.dumps(body),
        )

        def did_nack():
            return False

        channel.add_on_return_callback(did_nack)

        mock_should_execute.return_value = False

        dataset = Dataset(conn=conn, name=dataset_from_collection_id(collection_id))
        dataset.add_task(task_id, "test-op")
        status = dataset.get_active_dataset_status(conn=conn)
        stage = status["datasets"]["2"]["stages"][0]
        assert stage["pending"] == 1
        assert stage["running"] == 0

        worker = CountingWorker(queues=[test_queue_name], conn=conn, num_threads=1)
        assert not dataset.should_execute(task_id=task_id)
        with patch.object(
            worker,
            attribute="dispatch_task",
            return_value=None,
        ) as dispatch_fn:
            with patch.object(
                channel,
                attribute="basic_nack",
                return_value=None,
            ) as nack_fn:
                worker.process(blocking=False)
                nack_fn.assert_called_once()
                dispatch_fn.assert_not_called()

        status = dataset.get_active_dataset_status(conn=conn)
        stage = status["datasets"]["2"]["stages"][0]
        assert stage["pending"] == 1
        assert stage["running"] == 0
        assert dataset.is_task_tracked(Task(**body))


def test_dataset_get_status():
    conn = get_fakeredis()
    conn.flushdb()

    dataset = Dataset(conn=conn, name="123")
    status = dataset.get_status()

    assert status["pending"] == 0
    assert status["running"] == 0
    assert status["finished"] == 0
    assert status["start_time"] is None
    assert status["last_update"] is None

    task_one = Task(
        task_id="1",
        job_id="abc",
        delivery_tag="",
        operation="ingest",
        context={},
        payload={},
        priority=5,
        collection_id="1",
    )

    task_two = Task(
        task_id="2",
        job_id="abc",
        delivery_tag="",
        operation="ingest",
        context={},
        payload={},
        priority=5,
        collection_id="1",
    )

    task_three = Task(
        task_id="3",
        job_id="abc",
        delivery_tag="",
        operation="index",
        context={},
        payload={},
        priority=5,
        collection_id="1",
    )

    # Adding a task updates `start_time` and `last_update`
    with time_machine.travel("2024-01-01T00:00:00"):
        dataset.add_task(task_one.task_id, task_one.operation)

    status = dataset.get_status()
    assert status["pending"] == 1
    assert status["running"] == 0
    assert status["finished"] == 0
    assert status["start_time"].startswith("2024-01-01T00:00:00")
    assert status["last_update"].startswith("2024-01-01T00:00:00")

    # Once a worker starts processing a task, only `last_update` is updated
    with time_machine.travel("2024-01-02T00:00:00"):
        dataset.checkout_task(task_one.task_id, task_one.operation)

    status = dataset.get_status()
    assert status["pending"] == 0
    assert status["running"] == 1
    assert status["finished"] == 0
    assert status["start_time"].startswith("2024-01-01T00:00:00")
    assert status["last_update"].startswith("2024-01-02T00:00:00")

    # When another task is added, only `last_update` is updated
    with time_machine.travel("2024-01-03T00:00:00"):
        dataset.add_task(task_two.task_id, task_two.operation)

    status = dataset.get_status()
    assert status["pending"] == 1
    assert status["running"] == 1
    assert status["finished"] == 0
    assert status["start_time"].startswith("2024-01-01T00:00:00")
    assert status["last_update"].startswith("2024-01-03T00:00:00")

    # When the first task has been processed, `last_update` is updated
    with time_machine.travel("2024-01-04T00:00:00"):
        dataset.mark_done(task_one)

    status = dataset.get_status()
    assert status["pending"] == 1
    assert status["running"] == 0
    assert status["finished"] == 1
    assert status["start_time"].startswith("2024-01-01T00:00:00")
    assert status["last_update"].startswith("2024-01-04T00:00:00")

    # When the worker starts processing the second task, only `last_update` is updated
    with time_machine.travel("2024-01-05T00:00:00"):
        dataset.checkout_task(task_two.task_id, task_two.operation)

    status = dataset.get_status()
    assert status["pending"] == 0
    assert status["running"] == 1
    assert status["finished"] == 1
    assert status["start_time"].startswith("2024-01-01T00:00:00")
    assert status["last_update"].startswith("2024-01-05T00:00:00")

    # Once all tasks have been processed, status data is flushed
    with time_machine.travel("2024-01-06T00:00:00"):
        dataset.mark_done(task_two)

    status = dataset.get_status()
    assert status["pending"] == 0
    assert status["running"] == 0
    assert status["finished"] == 0
    assert status["start_time"] is None
    assert status["last_update"] is None

    # Adding a new task to an inactive dataset sets `start_time`
    with time_machine.travel("2024-01-07T00:00:00"):
        dataset.add_task(task_three.task_id, task_three.operation)

    status = dataset.get_status()
    assert status["pending"] == 1
    assert status["running"] == 0
    assert status["finished"] == 0
    assert status["start_time"].startswith("2024-01-07T00:00:00")
    assert status["last_update"].startswith("2024-01-07T00:00:00")

    # Cancelling a dataset flushes status data
    with time_machine.travel("2024-01-08T00:00:00"):
        dataset.checkout_task(task_three.task_id, task_three.operation)
        dataset.cancel()

    status = dataset.get_status()
    assert status["pending"] == 0
    assert status["running"] == 0
    assert status["finished"] == 0
    assert status["start_time"] is None
    assert status["last_update"] is None

    # Tasks that were already running when the dataset was cancelled
    # have no effect
    with time_machine.travel("2024-01-09T00:00:00"):
        dataset.mark_done(task_three)

    assert status["pending"] == 0
    assert status["running"] == 0
    assert status["finished"] == 0
    assert status["start_time"] is None
    assert status["last_update"] is None


def test_dataset_cancel():
    conn = get_fakeredis()
    conn.flushdb()

    dataset = Dataset(conn=conn, name="abc")
    assert conn.keys() == []

    # Enqueueing tasks stores status data in Redis
    dataset.add_task("1", "ingest")
    dataset.add_task("2", "index")
    dataset.checkout_task("1", "ingest")
    assert conn.keys() != []

    # Cancelling a dataset removes associated data from Redis
    dataset.cancel()
    assert conn.keys() == []


def test_dataset_mark_done():
    conn = get_fakeredis()
    conn.flushdb()

    dataset = Dataset(conn=conn, name="abc")
    assert conn.keys() == []

    task = Task(
        task_id="1",
        job_id="abc",
        delivery_tag="",
        operation="ingest",
        context={},
        payload={},
        priority=5,
        collection_id="abc",
    )

    # Enqueueing a task stores status data in Redis
    dataset.add_task(task.task_id, task.operation)
    dataset.checkout_task(task.task_id, task.operation)
    assert conn.keys() != []

    # Marking the last task as done cleans up status data in Redis
    dataset.mark_done(task)
    assert conn.keys() == []


@pytest.fixture
def prom_registry():
    # This relies on internal implementation details of the client to reset
    # previously collected metrics before every test execution. Unfortunately,
    # there is no clean way of achieving the same thing that doesn't add a lot
    # of complexity to the test and application code.
    collectors = REGISTRY._collector_to_names.keys()
    for collector in collectors:
        if isinstance(collector, MetricWrapperBase):
            collector._metrics.clear()
            collector._metric_init()

    yield REGISTRY


def test_prometheus_metrics_succeeded(prom_registry):
    conn = get_fakeredis()
    rmq_channel = get_rabbitmq_channel()
    worker = CountingWorker(conn=conn, queues=["ingest"], num_threads=1)
    declare_rabbitmq_queue(channel=rmq_channel, queue="ingest")

    queue_task(
        rmq_channel=rmq_channel,
        redis_conn=conn,
        collection_id=123,
        stage="ingest",
    )
    worker.process(blocking=False)

    started = prom_registry.get_sample_value(
        "servicelayer_tasks_started_total",
        {"stage": "ingest"},
    )
    assert started == 1

    succeeded = prom_registry.get_sample_value(
        "servicelayer_tasks_succeeded_total",
        {"stage": "ingest", "retries": "0"},
    )
    assert succeeded == 1

    # Under the hood, histogram metrics create multiple time series tracking
    # the number and sum of observations, as well as individual histogram buckets.
    duration_sum = prom_registry.get_sample_value(
        "servicelayer_task_duration_seconds_sum",
        {"stage": "ingest"},
    )
    duration_count = prom_registry.get_sample_value(
        "servicelayer_task_duration_seconds_count",
        {"stage": "ingest"},
    )
    assert duration_sum > 0
    assert duration_count == 1


def test_prometheus_metrics_failed(prom_registry):
    conn = get_fakeredis()
    rmq_channel = get_rabbitmq_channel()
    worker = FailingWorker(conn=conn, queues=["ingest"], num_threads=1)
    declare_rabbitmq_queue(channel=rmq_channel, queue="ingest")

    queue_task(
        rmq_channel=rmq_channel,
        redis_conn=conn,
        collection_id=123,
        stage="ingest",
    )
    worker.process(blocking=False)

    started = prom_registry.get_sample_value(
        "servicelayer_tasks_started_total",
        {"stage": "ingest"},
    )
    assert settings.WORKER_RETRY == 3
    assert started == 4  # Initial attempt + 3 retries

    first_attempt = REGISTRY.get_sample_value(
        "servicelayer_tasks_failed_total",
        {
            "stage": "ingest",
            "retries": "0",
            "failed_permanently": "False",
        },
    )
    assert first_attempt == 1

    last_attempt = REGISTRY.get_sample_value(
        "servicelayer_tasks_failed_total",
        {
            "stage": "ingest",
            "retries": "3",
            "failed_permanently": "True",
        },
    )
    assert last_attempt == 1


def test_get_priority_bucket():
    redis = get_fakeredis()
    rmq_channel = get_rabbitmq_channel()
    rmq_channel.queue_delete("index")
    declare_rabbitmq_queue(rmq_channel, "index")
    flush_queues(rmq_channel, redis, ["index"])
    collection_id = 1

    assert get_task_count(collection_id, redis) == 0
    assert get_priority(collection_id, redis) in (7, 8)

    queue_task(rmq_channel, redis, collection_id, "index")

    assert get_task_count(collection_id, redis) == 1
    assert get_priority(collection_id, redis) in (7, 8)

    with patch.object(
        Dataset,
        "get_active_dataset_status",
        return_value={
            "total": 9999,
            "datasets": {
                "1": {
                    "finished": 9999,
                    "running": 0,
                    "pending": 0,
                    "stages": [
                        {
                            "job_id": "",
                            "stage": "index",
                            "pending": 0,
                            "running": 0,
                            "finished": 9999,
                        }
                    ],
                    "start_time": "2024-06-25T10:58:49.779811",
                    "last_update": "2024-06-25T10:58:49.779819",
                }
            },
        },
    ):
        assert get_task_count(collection_id, redis) == 9999
        assert get_priority(collection_id, redis) in (4, 5, 6)

    with patch.object(
        Dataset,
        "get_active_dataset_status",
        return_value={
            "total": 10001,
            "datasets": {
                "1": {
                    "finished": 10000,
                    "running": 0,
                    "pending": 1,
                    "stages": [
                        {
                            "job_id": "",
                            "stage": "index",
                            "pending": 10001,
                            "running": 0,
                            "finished": 0,
                        }
                    ],
                    "start_time": "2024-06-25T10:58:49.779811",
                    "last_update": "2024-06-25T10:58:49.779819",
                }
            },
        },
    ):
        assert get_task_count(collection_id, redis) == 10001
        assert get_priority(collection_id, redis) in (1, 2, 3)


def test_get_priority_lists():
    redis = get_fakeredis()
    collection_id = 1

    assert Dataset.is_high_prio(redis, collection_id) is False
    assert Dataset.is_low_prio(redis, collection_id) is False

    redis.sadd("tq:prio:low", "1")
    redis.sadd("tq:prio:high", "2")

    assert Dataset.is_low_prio(redis, 1) is True
    assert Dataset.is_high_prio(redis, 1) is False

    assert Dataset.is_low_prio(redis, 2) is False
    assert Dataset.is_high_prio(redis, 2) is True

    assert get_priority(1, redis) == 0
    assert get_priority(2, redis) == 9
