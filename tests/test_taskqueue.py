import datetime
from unittest import TestCase
from unittest.mock import patch
from unittest import skip
import json
from random import randrange

import pika

from servicelayer import settings
from servicelayer.cache import get_fakeredis
from servicelayer.taskqueue import (
    Worker,
    Dataset,
    Task,
    get_rabbitmq_connection,
    dataset_from_collection_id,
    declare_rabbitmq_queue,
)
from servicelayer.util import unpack_datetime


class CountingWorker(Worker):
    def dispatch_task(self, task):
        assert isinstance(task, Task), task
        if not hasattr(self, "test_done"):
            self.test_done = 0
        self.test_done += 1
        self.test_task = task


class TaskQueueTest(TestCase):
    def test_task_queue(self):
        test_queue_name = "sls-queue-ingest"
        conn = get_fakeredis()
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
        connection = get_rabbitmq_connection()
        channel = connection.channel()
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
            channel = connection.channel()
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
                worker.process(blocking=False)
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

    @skip("Unfinished")
    @patch("servicelayer.taskqueue.Dataset.should_execute")
    def test_task_that_shouldnt_execute(self, mock_should_execute):
        test_queue_name = "sls-queue-ingest"
        conn = get_fakeredis()
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
        connection = get_rabbitmq_connection()
        channel = connection.channel()
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

        worker = CountingWorker(queues=[test_queue_name], conn=conn, num_threads=1)
        assert not dataset.should_execute(task_id=task_id)
        worker.process(blocking=False)

        channel.close()
