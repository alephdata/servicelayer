from dataclasses import dataclass
from typing import Optional
import json
import time
import threading
import signal
import logging
import sys
from abc import ABC, abstractmethod
import functools
from queue import Queue, Empty

from structlog.contextvars import clear_contextvars, bind_contextvars
import pika
from banal import ensure_list

from servicelayer.cache import get_redis, make_key
from servicelayer.util import unpack_int
from servicelayer import settings
from servicelayer.util import service_retries, backoff

log = logging.getLogger(__name__)
local = threading.local()


PREFIX = "tq"
NO_COLLECTION = "null"


OP_INGEST = "ingest"
OP_ANALYZE = "analyze"
OP_INDEX = "index"

# ToDo: consider ALEPH_INGEST_PIPELINE setting
INGEST_OPS = (OP_INGEST, OP_ANALYZE)

TIMEOUT = 5


@dataclass
class Task:
    task_id: str
    job_id: str
    delivery_tag: str
    operation: str
    context: dict
    payload: dict
    collection_id: Optional[str] = None

    @property
    def retry_key(self):
        dataset = dataset_from_collection_id(self.collection_id)
        return make_key(PREFIX, "qdj", dataset, "taskretry", self.task_id)

    def get_retry_count(self, conn):
        return unpack_int(conn.get(self.retry_key))

    def increment_retry_count(self, conn):
        conn.incr(self.retry_key)
        conn.expire(self.retry_key, settings.REDIS_EXPIRE)

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

    @classmethod
    def cleanup_dataset_status(cls, conn):
        """Clean up dataset status for inactive datasets."""
        datasets_key = make_key(PREFIX, "qdatasets")
        for name in conn.smembers(datasets_key):
            dataset = cls(conn, name)
            status = dataset.get_status()
            if status["running"] == 0 and status["pending"] == 0:
                pipe = conn.pipeline()
                # remove the dataset from active datasets
                pipe.srem(dataset.key, dataset.name)
                # reset finished task count
                pipe.delete(dataset.finished_key)
                pipe.execute()

    def should_execute(self, task_id):
        """Should a task be executed?

        When a the processing of a task is cancelled, there is no way to tell RabbitMQ to drop it.
        So we store that state in Redis and make the worker check with Redis before executing a task.
        """
        attempt = 1
        while True:
            _should_execute = self.conn.sismember(
                self.pending_key, task_id
            ) or self.conn.sismember(self.running_key, task_id)
            if not _should_execute and attempt < settings.WORKER_RETRY:
                # Sometimes for new tasks the check fails because the Redis
                # state gets updated only after the task gets written to disk
                # by RabbitMQ whereas the worker consumer gets the task before
                # that.
                # Retry a few times to avoid those cases.
                backoff(failures=attempt)
                attempt += 1
                continue
            return _should_execute

    def add_task(self, task_id):
        """Update state when a new task is added to the task queue"""
        log.info(f"Adding task: {task_id}")
        pipe = self.conn.pipeline()
        # add the dataset to active datasets
        pipe.sadd(self.key, self.name)
        pipe.sadd(self.pending_key, task_id)
        pipe.execute()

    def remove_task(self, task_id):
        """Remove a task that's not going to be executed"""
        log.info(f"Removing task: {task_id}")
        self.conn.srem(self.pending_key, task_id)
        status = self.get_status()
        if status["running"] == 0 and status["pending"] == 0:
            # remove the dataset from active datasets
            self.conn.srem(self.key, self.name)

    def checkout_task(self, task_id):
        """Update state when a task is checked out for execution"""
        log.info(f"Checking out task: {task_id}")
        pipe = self.conn.pipeline()
        # add the dataset to active datasets
        pipe.sadd(self.key, self.name)
        pipe.srem(self.pending_key, task_id)
        pipe.sadd(self.running_key, task_id)
        pipe.execute()

    def mark_done(self, task: Task):
        """Update state when a task is finished executing"""
        log.info(f"Finished executing task: {task.task_id}")
        pipe = self.conn.pipeline()
        pipe.srem(self.pending_key, task.task_id)
        pipe.srem(self.running_key, task.task_id)
        pipe.incr(self.finished_key)
        pipe.delete(task.retry_key)
        pipe.execute()
        status = self.get_status()
        if status["running"] == 0 and status["pending"] == 0:
            # remove the dataset from active datasets
            self.conn.srem(self.key, self.name)

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
        routing_key = settings.QUEUE_INGEST
    elif stage == OP_INDEX:
        routing_key = settings.QUEUE_INDEX
    else:
        routing_key = settings.QUEUE_ALEPH
    return routing_key


class MaxRetriesExceededError(Exception):
    pass


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
        self.queues = ensure_list(queues)
        self.version = version
        self.prefetch_count = prefetch_count
        self.local_queue = Queue()

    def on_signal(self, signal, _):
        log.warning(f"Shutting down worker (signal {signal})")
        # Exit eagerly without waiting for current task to finish running
        sys.exit(int(signal))

    def on_message(self, channel, method, properties, body, args):
        """RabbitMQ on_message event handler.

        We have to make sure it doesn't block for long to ensure that RabbitMQ
        heartbeats are not interrupted.
        """
        connection = args[0]
        task = get_task(body, method.delivery_tag)
        self.local_queue.put((task, channel, connection))

    def process_blocking(self):
        """Blocking worker thread - executes tasks from a queue and periodic tasks"""
        while True:
            try:
                (task, channel, connection) = self.local_queue.get(timeout=TIMEOUT)
                apply_task_context(task, v=self.version)
                self.handle(task)
                cb = functools.partial(self.ack_message, task, channel)
                connection.add_callback_threadsafe(cb)
            except Empty:
                pass
            finally:
                clear_contextvars()
                self.periodic()

    def process_nonblocking(self):
        """Non-blocking worker is used for tests only."""
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        queue_active = {queue: True for queue in self.queues}
        while True:
            for queue in self.queues:
                method, properties, body = channel.basic_get(queue=queue)
                if method is None:
                    queue_active[queue] = False
                    # Quit processing if all queues are inactive
                    if not any(queue_active.values()):
                        return
                else:
                    queue_active[queue] = True
                    task = get_task(body, method.delivery_tag)
                    self.handle(task)

    def process(self, blocking=True):
        if blocking:
            self.process_blocking()
        else:
            self.process_nonblocking()

    def handle(self, task: Task):
        """Execute a task."""
        try:
            dataset = Dataset(
                conn=self.conn, name=dataset_from_collection_id(task.collection_id)
            )
            if dataset.should_execute(task.task_id):
                if task.get_retry_count(self.conn) > settings.WORKER_RETRY:
                    raise MaxRetriesExceededError(
                        f"Max retries reached for task {task.task_id}. Aborting."
                    )
                dataset.checkout_task(task.task_id)
                task.increment_retry_count(self.conn)
                task = self.dispatch_task(task)
            else:
                log.warn(f"Discarding task: {task.task_id}")
        except Exception:
            log.exception("Error in task handling")
        finally:
            self.after_task(task)

    @abstractmethod
    def dispatch_task(self, task: Task) -> Task:
        raise NotImplementedError

    def after_task(self, task: Task):
        """Run after-task clean up"""
        pass

    def periodic(self):
        """Periodic tasks to run."""
        pass

    def ack_message(self, task, channel):
        """Acknowledge a task after execution.

        RabbitMQ requires that the channel used for receiving the message must be used
        for acknowledging a message as well.
        """
        apply_task_context(task, v=self.version)
        skip_ack = task.context.get("skip_ack")
        if skip_ack:
            log.info(
                f"Skipping acknowledging message {task.delivery_tag} for task_id {task.task_id}"
            )
        else:
            log.info(
                f"Acknowledging message {task.delivery_tag} for task_id {task.task_id}"
            )
            dataset = task.get_dataset(conn=self.conn)
            # Sync state to redis
            dataset.mark_done(task)
            if channel.is_open:
                channel.basic_ack(task.delivery_tag)
        clear_contextvars()

    def run(self):
        """Run a blocking worker instance"""

        # Handle kill signals
        signal.signal(signal.SIGINT, self.on_signal)
        signal.signal(signal.SIGTERM, self.on_signal)

        # worker threads
        process = lambda: self.process(blocking=True)
        threads = []
        for _ in range(self.num_threads):
            thread = threading.Thread(target=process)
            thread.daemon = True
            thread.start()
            threads.append(thread)

        log.info(f"Worker has {self.num_threads} worker threads.")

        connection = get_rabbitmq_connection()
        channel = connection.channel()
        channel.basic_qos(prefetch_count=self.prefetch_count)
        on_message_callback = functools.partial(self.on_message, args=(connection,))
        for queue in self.queues:
            channel.queue_declare(queue=queue, durable=True)
            channel.basic_consume(queue=queue, on_message_callback=on_message_callback)
        channel.start_consuming()


def get_rabbitmq_connection():
    for attempt in service_retries():
        try:
            if not hasattr(local, "connection") or not local.connection:
                credentials = pika.PlainCredentials(
                    settings.RABBITMQ_USERNAME, settings.RABBITMQ_PASSWORD
                )
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=settings.RABBITMQ_URL,
                        credentials=credentials,
                        heartbeat=settings.RABBITMQ_HEARTBEAT,
                        blocked_connection_timeout=settings.RABBITMQ_BLOCKED_CONNECTION_TIMEOUT,
                    )
                )
                local.connection = connection
            if local.connection.is_open:
                channel = local.connection.channel()
                channel.queue_declare(queue=settings.QUEUE_ALEPH, durable=True)
                channel.queue_declare(queue=settings.QUEUE_INGEST, durable=True)
                channel.queue_declare(queue=settings.QUEUE_INDEX, durable=True)
                channel.close()
                return local.connection
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPError):
            log.exception("RabbitMQ error")
        local.connection = None
        backoff(failures=attempt)
    raise RuntimeError("Could not connect to RabbitMQ")
