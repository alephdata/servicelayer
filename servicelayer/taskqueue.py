from dataclasses import dataclass
from typing import Optional
import json
import time
import threading
import signal
import logging
import sys

from structlog.contextvars import clear_contextvars, bind_contextvars
import pika

from servicelayer.cache import get_redis, make_key
from servicelayer.util import unpack_int
from servicelayer import settings

log = logging.getLogger(__name__)


PREFIX = "tq"
NO_COLLECTION = "null"

QUEUE_ALEPH = "aleph_queue"
QUEUE_INGEST = "ingest_queue"


@dataclass
class Task:
    task_id: str
    job_id: str
    delivery_tag: str
    operation: str
    context: dict
    payload: dict
    collection_id: Optional[str] = None

    def get_dataset(self, conn):
        dataset = Dataset(
            conn=conn, name=dataset_from_collection_id(self.collection_id)
        )
        return dataset


class Dataset(object):
    """Track the status of tasks for a given dataset/collection"""

    def __init__(self, conn, name):
        self.conn = conn
        self.name = name
        self.key = make_key(PREFIX, "qdatasets")
        # integer that tracks number of finished tasks
        self.finished_key = make_key(PREFIX, "qdj", name, "finished")
        # sets that contain task ids of running and pending tasks
        self.running_key = make_key(PREFIX, "qdj", name, "running")
        self.pending_key = make_key(PREFIX, "qdj", name, "pending")

    def cancel(self):
        """Cancel processing of all tasks belonging to a dataset"""
        pipe = self.conn.pipeline()
        # remove the dataset from active datasets
        pipe.srem(self.key, self.name)
        # clean up tasks and task counts
        pipe.delete(self.finished_key)
        pipe.delete(self.running_key)
        pipe.delete(self.pending_key)
        pipe.execute()

    def get_status(self):
        """Status of a given dataset."""
        status = {"finished": 0, "running": 0, "pending": 0}
        finished = self.conn.get(self.finished_key)
        running = self.conn.scard(self.running_key)
        pending = self.conn.scard(self.pending_key)
        status["finished"] = max(0, unpack_int(finished))
        status["running"] = max(0, unpack_int(running))
        status["pending"] = max(0, unpack_int(pending))
        return status

    @classmethod
    def get_active_dataset_status(cls, conn):
        """Status of all active datasets."""
        result = {"total": 0, "datasets": {}}
        datasets_key = make_key(PREFIX, "qdatasets")
        for name in conn.smembers(datasets_key):
            dataset = cls(conn, name)
            status = dataset.get_status()
            result["total"] += 1
            result["datasets"][dataset.name] = status
        return result

    def should_execute(self, task_id):
        """Should a task be executed?

        When a the processing of a task is cancelled, there is no way to tell RabbitMQ to drop it.
        So we store that state in Redis and make the worker check with Redis before executing a task.
        """
        return self.conn.sismember(self.pending_key, task_id) or self.conn.sismember(
            self.running_key, task_id
        )

    def add_task(self, task_id):
        """Update state when a new task is added to the task queue"""
        pipe = self.conn.pipeline()
        # add the dataset to active datasets
        pipe.sadd(self.key, self.name)
        pipe.sadd(self.pending_key, task_id)
        pipe.execute()

    def checkout_task(self, task_id):
        """Update state when a task is checked out for execution"""
        pipe = self.conn.pipeline()
        # add the dataset to active datasets
        pipe.sadd(self.key, self.name)
        pipe.srem(self.pending_key, task_id)
        pipe.sadd(self.running_key, task_id)
        pipe.execute()

    def mark_done(self, task_id):
        """Update state when a task is finished executing"""
        status = self.get_status()
        pipe = self.conn.pipeline()
        if status["running"] == 0 and status["pending"] == 0:
            # remove the dataset from active datasets
            pipe.srem(self.key, self.name)
        pipe.srem(self.running_key, task_id)
        pipe.incr(self.finished_key)
        pipe.execute()

    def is_done(self):
        status = self.get_status()
        return status["pending"] == 0 and status["running"] == 0

    def __str__(self):
        return self.name


def get_task(body, delivery_tag) -> Task:
    body = json.loads(body)
    return Task(
        collection_id=collection_id_from_dataset(body["collection_id"]),
        task_id=body["task_id"],
        job_id=body["job_id"],
        delivery_tag=delivery_tag,
        operation=body["operation"],
        context=body["context"] or {},
        payload=body["payload"] or {},
    )


def dataset_from_collection_id(collection_id):
    """servicelayer dataset from a collection"""
    if collection_id is None:
        return NO_COLLECTION
    return str(collection_id)


def collection_id_from_dataset(dataset):
    """Invert the servicelayer dataset into a collection ID"""
    if dataset == NO_COLLECTION:
        return None
    return int(dataset)


def apply_task_context(task: Task, **kwargs):
    """This clears the current structured logging context and readies it
    for a new task"""
    # Setup context for structured logging
    clear_contextvars()
    bind_contextvars(
        job_id=task.job_id,
        stage=task.operation,
        dataset=dataset_from_collection_id(task.collection_id),
        start_time=time.time(),
        trace_id=task.task_id,
        **kwargs,
    )


class Worker:
    def __init__(
        self, queue_name, conn=None, num_threads=settings.WORKER_THREADS, version=None
    ):
        self.conn = conn or get_redis()
        self.num_threads = num_threads
        self.threadlocal = threading.local()
        self.threadlocal._channel = None
        self.queue_name = queue_name
        self.version = version

    def on_signal(self, signal, frame):
        log.warning(f"Shutting down worker (signal {signal})")
        # Exit eagerly without waiting for current task to finish running
        sys.exit(int(signal))

    def connect(self):
        # Pika connections should not be shared between threads. So we use a
        # threadlocal connection object

        self.threadlocal._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=settings.RABBITMQ_URL)
        )
        self.threadlocal._channel = self.threadlocal._connection.channel()
        self.threadlocal._channel.confirm_delivery()

    def on_message(self, _, method, properties, body):
        log.info(f"Received message # {method.delivery_tag}: {body}")
        task = get_task(body, method.delivery_tag)
        dataset = Dataset(
            conn=self.conn, name=dataset_from_collection_id(task.collection_id)
        )
        if dataset.should_execute(task.task_id):
            dataset.checkout_task(task.task_id)
            self.handle(task)

    def process_blocking(self):
        # Recover from connection errors: https://github.com/pika/pika#connection-recovery
        while True:
            try:
                self.connect()
                self.threadlocal._channel.queue_declare(
                    queue=self.queue_name, durable=True
                )
                self.threadlocal._channel.basic_qos(prefetch_count=1)
                self.threadlocal._channel.basic_consume(
                    queue=self.queue_name, on_message_callback=self.on_message
                )
                self.threadlocal._channel.start_consuming()
            # Don't recover if connection was closed by broker
            except pika.exceptions.ConnectionClosedByBroker:
                break
            # Don't recover on channel errors
            except pika.exceptions.AMQPChannelError:
                break
            # Recover on all other connection errors
            except pika.exceptions.AMQPConnectionError:
                continue

    def process_nonblocking(self):
        # non-blocking worker is used for tests only. We can go easy on connection recovery
        if not self.threadlocal._channel:
            self.connect()
        self.threadlocal._channel.queue_declare(queue=self.queue_name, durable=True)
        self.threadlocal._channel.basic_qos(prefetch_count=1)
        while True:
            method, properties, body = self.threadlocal._channel.basic_get(
                queue=self.queue_name
            )
            if method is None:
                return
            self.on_message(None, method, properties, body)

    def process(self, blocking=True):
        if blocking:
            self.process_blocking()
        else:
            self.process_nonblocking()

    def handle(self, task):
        # ToDo: handle retries
        try:
            apply_task_context(task, v=self.version)
            self.dispatch_task(task)
        except Exception:
            log.exception("Error in task handling")
        finally:
            self.after_task(task)

    def after_task(self, task):
        log.info(f"Acknowledging message {task.delivery_tag} {task.task_id}")
        self.threadlocal._channel.basic_ack(task.delivery_tag)
        dataset = task.get_dataset(conn=self.conn)
        dataset.mark_done(task.task_id)

    def run(self, blocking=True, interval=None):
        signal.signal(signal.SIGINT, self.on_signal)
        signal.signal(signal.SIGTERM, self.on_signal)
        process = lambda: self.process(blocking=blocking, interval=interval)
        if not self.num_threads:
            return process()
        log.info("Worker has %d threads.", self.num_threads)
        threads = []
        for _ in range(self.num_threads):
            thread = threading.Thread(target=process)
            thread.daemon = True
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()
