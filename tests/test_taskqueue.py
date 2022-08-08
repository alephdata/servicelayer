from unittest import TestCase
from unittest.mock import patch
import json


from servicelayer import settings
from servicelayer.cache import get_fakeredis
from servicelayer.taskqueue import (
    Worker,
    Dataset,
    Task,
    get_rabbitmq_connection,
    dataset_from_collection_id,
)


class CountingWorker(Worker):
    def dispatch_task(self, task):
        assert isinstance(task, Task), task
        if not hasattr(self, "test_done"):
            self.test_done = 0
        self.test_done += 1
        self.test_task = task


class TaskQueueTest(TestCase):
    def test_task_queue(self):
        settings.QUEUE_INGEST = "sls-queue-ingest"
        conn = get_fakeredis()
        collection_id = 2
        task_id = "test-task"
        body = {
            "collection_id": 2,
            "job_id": "test-job",
            "task_id": "test-task",
            "operation": "test-op",
            "context": {},
            "payload": {},
        }
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        channel.queue_purge(settings.QUEUE_INGEST)
        channel.basic_publish(
            exchange="",
            routing_key=settings.QUEUE_INGEST,
            body=json.dumps(body),
        )
        dataset = Dataset(conn=conn, name=dataset_from_collection_id(collection_id))
        dataset.add_task(task_id)
        channel.close()

        status = dataset.get_status()
        assert status["finished"] == 0, status
        assert status["pending"] == 1, status
        assert status["running"] == 0, status

        worker = CountingWorker(
            queues=[settings.QUEUE_INGEST], conn=conn, num_threads=1
        )
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
            channel.queue_purge(settings.QUEUE_INGEST)
            channel.basic_publish(
                exchange="",
                routing_key=settings.QUEUE_INGEST,
                body=json.dumps(body),
            )
            dataset = Dataset(conn=conn, name=dataset_from_collection_id(collection_id))
            dataset.add_task(task_id)
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
