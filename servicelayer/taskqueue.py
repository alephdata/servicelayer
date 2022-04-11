from dataclasses import dataclass
from typing import Optional
import json
import time
import threading
import signal
import logging
import sys
from abc import ABC, abstractmethod

from structlog.contextvars import clear_contextvars, bind_contextvars
import pika
from banal import ensure_list

from servicelayer.cache import get_redis, make_key
from servicelayer.util import unpack_int
from servicelayer import settings
from servicelayer.util import service_retries, backoff

log = logging.getLogger(__name__)


PREFIX = "tq"
NO_COLLECTION = "null"

QUEUE_ALEPH = "aleph_queue"
QUEUE_INGEST = "ingest_queue"
QUEUE_INDEX = "index_queue"

OP_INGEST = "ingest"
OP_ANALYZE = "analyze"
OP_INDEX = "index"

# ToDo: consider ALEPH_INGEST_PIPELINE setting
INGEST_OPS = (OP_INGEST, OP_ANALYZE)


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


class Dataset:
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


def get_routing_key(stage):
    if stage in INGEST_OPS:
        routing_key = QUEUE_INGEST
    elif stage == OP_INDEX:
        routing_key = QUEUE_INDEX
    else:
        routing_key = QUEUE_ALEPH
    return routing_key


class Worker(ABC):
    def __init__(
        self,
        queues,
        conn=None,
        num_threads=settings.WORKER_THREADS,
        version=None,
        prefetch_count=100,
    ):
        self.conn = conn or get_redis()
        self.num_threads = num_threads
        self.threadlocal = threading.local()
        self.threadlocal._channel = None
        self.queues = ensure_list(queues)
        self.version = version
        self.prefetch_count = prefetch_count

    def on_signal(self, signal, _):
        log.warning(f"Shutting down worker (signal {signal})")
        # Exit eagerly without waiting for current task to finish running
        sys.exit(int(signal))

    def connect(self):
        # Pika channels should not be shared between threads. So we use a
        # threadlocal channel object
        self.threadlocal._channel = get_rabbitmq_channel()
        self.threadlocal._channel.queue_declare(queue=QUEUE_ALEPH, durable=True)
        self.threadlocal._channel.queue_declare(queue=QUEUE_INGEST, durable=True)
        self.threadlocal._channel.queue_declare(queue=QUEUE_INDEX, durable=True)
        self.threadlocal._channel.basic_qos(prefetch_count=self.prefetch_count)

    def on_message(self, _, method, properties, body):
        log.debug(f"Received message # {method.delivery_tag}: {body}")
        task = get_task(body, method.delivery_tag)
        dataset = Dataset(
            conn=self.conn, name=dataset_from_collection_id(task.collection_id)
        )
        dataset.checkout_task(task.task_id)
        if dataset.should_execute(task.task_id):
            self.handle(task)

    def process_blocking(self):
        # Recover from connection errors: https://github.com/pika/pika#connection-recovery
        while True:
            try:
                self.connect()
                for queue in self.queues:
                    self.threadlocal._channel.queue_declare(queue=queue, durable=True)
                    self.threadlocal._channel.basic_consume(
                        queue=queue, on_message_callback=self.on_message
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
        queue_active = {queue: True for queue in self.queues}
        while True:
            for queue in self.queues:
                method, properties, body = self.threadlocal._channel.basic_get(
                    queue=queue
                )
                if method is None:
                    queue_active[queue] = False
                    # Quit processing if all queues are inactive
                    if not any(queue_active.values()):
                        return
                else:
                    queue_active[queue] = True
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
            task = self.dispatch_task(task)
        except Exception:
            log.exception("Error in task handling")
        finally:
            self.after_task(task)

    @abstractmethod
    def dispatch_task(self, task: Task) -> Task:
        raise NotImplementedError

    def after_task(self, task):
        skip_ack = task.context.get("skip_ack")
        if skip_ack:
            log.info(
                f"Skipping acknowledging message {task.delivery_tag} {task.task_id}"
            )
        else:
            log.info(f"Acknowledging message {task.delivery_tag} {task.task_id}")
            dataset = task.get_dataset(conn=self.conn)
            dataset.mark_done(task.task_id)
            self.threadlocal._channel.basic_ack(task.delivery_tag)

    def cron(self, interval=5):
        """Run periodic tasks every `interval` seconds

        Args:
            interval (int, optional): interval between subsequent runs. Defaults to 5.
        """
        while True:
            try:
                self.periodic()
                time.sleep(interval)
            except Exception as ex:
                log.exception(f"Error while running periodic tasks: {ex}")

    def periodic(self):
        """Periodic tasks to run."""
        pass

    def run(self, cron_interval=None):
        """Run a blocking worker instance

        Args:
            cron_interval (int, optional): If defined, the worker runs with a cron
            thread that executes periodic tasks every `cron_interval` seconds. Defaults to None.
        """

        # Handle kill signals
        signal.signal(signal.SIGINT, self.on_signal)
        signal.signal(signal.SIGTERM, self.on_signal)

        # Regular worker threads
        process = lambda: self.process(blocking=True)
        threads = []
        for _ in range(self.num_threads):
            thread = threading.Thread(target=process)
            thread.daemon = True
            thread.start()
            threads.append(thread)

        # Cron worker thread
        if cron_interval:
            cron = lambda: self.cron(interval=cron_interval)
            thread = threading.Thread(target=cron)
            thread.daemon = True
            thread.start()
            threads.append(thread)
            log.info(
                "Worker has %d worker threads and 1 cron thread.", self.num_threads
            )
        else:
            log.info("Worker has %d worker threads.", self.num_threads)

        for thread in threads:
            thread.join()


def get_rabbitmq_channel():
    for attempt in service_retries():
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=settings.RABBITMQ_URL)
            )
            channel = connection.channel()
            channel.confirm_delivery()
            return channel
        except pika.exceptions.AMQPConnectionError:
            log.info("Waiting for RabbitMQ to load...")
            backoff(failures=attempt)
    raise RuntimeError("RabbitMQ is not ready.")
