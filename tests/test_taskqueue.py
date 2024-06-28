import datetime
from unittest import TestCase
from unittest.mock import patch
import json
from random import randrange

import pika

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
        assert status["end_time"] is None
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
        assert status["finished"] == 1, status
        assert status["pending"] == 0, status
        assert status["running"] == 0, status
        started = unpack_datetime(status["start_time"])
        last_updated = unpack_datetime(status["last_update"])
        end_time = unpack_datetime(status["end_time"])
        assert started < end_time < last_updated

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
                    "end_time": None,
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
                    "end_time": None,
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
