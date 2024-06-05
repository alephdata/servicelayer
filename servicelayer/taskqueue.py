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
import platform
from collections import defaultdict
from threading import Thread
from timeit import default_timer

import pika.spec
from prometheus_client import start_http_server

from structlog.contextvars import clear_contextvars, bind_contextvars
import pika
from banal import ensure_list

from servicelayer.cache import get_redis, make_key
from servicelayer.util import pack_now, unpack_int
from servicelayer import settings
from servicelayer.util import service_retries, backoff
from servicelayer import metrics

log = logging.getLogger(__name__)
local = threading.local()

PREFIX = "tq"
NO_COLLECTION = "null"

TIMEOUT = 5


@dataclass
class Task:
    task_id: str
    job_id: str
    delivery_tag: str
    operation: str
    context: dict
    payload: dict
    priority: int
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
        self.start_key = make_key(PREFIX, "qdj", name, "start")
        self.end_key = make_key(PREFIX, "qdj", name, "end")
        self.last_update_key = make_key(PREFIX, "qdj", name, "last_update")
        self.active_stages_key = make_key(PREFIX, "qds", name, "active_stages")

    def cancel(self):
        """Cancel processing of all tasks belonging to a dataset"""
        pipe = self.conn.pipeline()
        # remove the dataset from active datasets
        pipe.srem(self.key, self.name)
        # clean up tasks and task counts
        pipe.delete(self.finished_key)
        pipe.delete(self.running_key)
        pipe.delete(self.pending_key)
        pipe.delete(self.start_key)
        pipe.delete(self.end_key)
        pipe.delete(self.last_update_key)
        for stage in self.conn.smembers(self.active_stages_key):
            stage_key = self.get_stage_key(stage)
            pipe.delete(stage_key)
            pipe.delete(make_key(stage_key, "pending"))
            pipe.delete(make_key(stage_key, "running"))
            pipe.delete(make_key(stage_key, "finished"))
        pipe.delete(self.active_stages_key)
        pipe.execute()

    def get_status(self):
        """Status of a given dataset."""
        status = {"finished": 0, "running": 0, "pending": 0, "stages": []}

        start, end, last_update = self.conn.mget(
            (self.start_key, self.end_key, self.last_update_key)
        )
        status["start_time"] = start
        status["end_time"] = end
        status["last_update"] = last_update

        for stage in self.conn.smembers(self.active_stages_key):
            stage_key = self.get_stage_key(stage)
            status["stages"].append(
                {
                    "job_id": "",
                    "stage": stage,
                    "pending": max(
                        0, unpack_int(self.conn.scard(make_key(stage_key, "pending")))
                    ),
                    "running": max(
                        0, unpack_int(self.conn.scard(make_key(stage_key, "running")))
                    ),
                    "finished": max(
                        0, unpack_int(self.conn.get(make_key(stage_key, "finished")))
                    ),
                }
            )

        status["finished"] = max(
            0, sum([stage["finished"] for stage in status["stages"]])
        )
        status["running"] = max(
            0, sum([stage["running"] for stage in status["stages"]])
        )
        status["pending"] = max(
            0, sum([stage["pending"] for stage in status["stages"]])
        )

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
                # delete information about running stages
                for stage in dataset.conn.smembers(dataset.active_stages_key):
                    stage_key = dataset.get_stage_key(stage)
                    pipe.delete(stage_key)
                    pipe.delete(make_key(stage_key, "pending"))
                    pipe.delete(make_key(stage_key, "running"))
                    pipe.delete(make_key(stage_key, "finished"))
                # delete stages key
                pipe.delete(dataset.active_stages_key)
                pipe.set(dataset.last_update_key, pack_now())

                pipe.execute()

    def should_execute(self, task_id):
        """Should a task be executed?

        When a the processing of a task is cancelled, there is no way
        to tell RabbitMQ to drop it. So we store that state in Redis
        and make the worker check with Redis before executing a task.
        """
        attempt = 1
        while True:
            _should_execute = self.conn.sismember(
                self.pending_key, task_id
            ) or self.conn.sismember(self.running_key, task_id)
            if not _should_execute and attempt - 1 in service_retries():
                # Sometimes for new tasks the check fails because the Redis
                # state gets updated only after the task gets written to disk
                # by RabbitMQ whereas the worker consumer gets the task before
                # that.
                # Retry a few times to avoid those cases.
                backoff(failures=attempt)
                attempt += 1
                continue
            return _should_execute

    def add_task(self, task_id, stage):
        """Update state when a new task is added to the task queue"""
        log.info(f"Adding task: {task_id}")
        pipe = self.conn.pipeline()
        # add the dataset to active datasets
        pipe.sadd(self.key, self.name)

        # update status of stages per dataset
        stage_key = self.get_stage_key(stage)
        pipe.sadd(self.active_stages_key, stage)
        pipe.sadd(stage_key, task_id)
        pipe.sadd(make_key(stage_key, "pending"), task_id)

        pipe.sadd(self.pending_key, task_id)
        pipe.set(self.start_key, pack_now())
        pipe.set(self.last_update_key, pack_now())
        pipe.delete(self.end_key)
        pipe.execute()

    def remove_task(self, task_id, stage):
        """Remove a task that's not going to be executed"""
        log.info(f"Removing task: {task_id}")
        pipe = self.conn.pipeline()
        pipe.srem(self.pending_key, task_id)

        stage_key = self.get_stage_key(stage)
        pipe.srem(stage_key, task_id)
        pipe.srem(make_key(stage_key, "pending"), task_id)

        pipe.delete(make_key(PREFIX, "qdj", self.name, "taskretry", task_id))

        status = self.get_status()
        if status["running"] == 0 and status["pending"] == 0:
            # remove the dataset from active datasets
            pipe.srem(self.key, self.name)
            # reset finished task count
            pipe.delete(self.finished_key)
            # delete information about running stages
            for stage in self.conn.smembers(self.active_stages_key):
                stage_key = self.get_stage_key(stage)
                pipe.delete(stage_key)
                pipe.delete(make_key(stage_key, "pending"))
                pipe.delete(make_key(stage_key, "running"))
                pipe.delete(make_key(stage_key, "finished"))
            # delete stages key
            pipe.delete(self.active_stages_key)
        pipe.set(self.last_update_key, pack_now())
        pipe.execute()

    def checkout_task(self, task_id, stage):
        """Update state when a task is checked out for execution"""
        log.info(f"Checking out task: {task_id}")
        pipe = self.conn.pipeline()
        # add the dataset to active datasets
        pipe.sadd(self.key, self.name)

        # update status of stages per dataset
        stage_key = self.get_stage_key(stage)
        pipe.sadd(self.active_stages_key, stage)
        pipe.sadd(stage_key, task_id)
        pipe.srem(make_key(stage_key, "pending"), task_id)
        pipe.sadd(make_key(stage_key, "running"), task_id)

        pipe.srem(self.pending_key, task_id)
        pipe.sadd(self.running_key, task_id)
        pipe.set(self.start_key, pack_now())
        pipe.set(self.last_update_key, pack_now())
        pipe.delete(self.end_key)
        pipe.execute()

    def mark_done(self, task: Task):
        """Update state when a task is finished executing"""
        log.info(f"Finished executing task: {task.task_id}")
        pipe = self.conn.pipeline()
        pipe.srem(self.pending_key, task.task_id)
        pipe.srem(self.running_key, task.task_id)
        pipe.incr(self.finished_key)
        pipe.delete(task.retry_key)

        stage_key = self.get_stage_key(task.operation)
        pipe.srem(stage_key, task.task_id)
        pipe.srem(make_key(stage_key, "pending"), task.task_id)
        pipe.srem(make_key(stage_key, "running"), task.task_id)
        pipe.incr(make_key(stage_key, "finished"))

        pipe.set(self.end_key, pack_now())
        pipe.set(self.last_update_key, pack_now())
        pipe.execute()
        status = self.get_status()
        if status["running"] == 0 and status["pending"] == 0:
            # remove the dataset from active datasets
            self.conn.srem(self.key, self.name)
            # reset finished task count
            pipe.delete(self.finished_key)
            # delete information about running stages
            for stage in self.conn.smembers(self.active_stages_key):
                stage_key = self.get_stage_key(stage)
                pipe.delete(stage_key)
                pipe.delete(make_key(stage_key, "pending"))
                pipe.delete(make_key(stage_key, "running"))
                pipe.delete(make_key(stage_key, "finished"))
            # delete stages key
            pipe.delete(self.active_stages_key)

    def is_done(self):
        status = self.get_status()
        return status["pending"] == 0 and status["running"] == 0

    def __str__(self):
        return self.name

    def get_stage_key(self, stage):
        return make_key(PREFIX, "qds", self.name, stage)


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
        priority=body["priority"] or 0,
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


def declare_rabbitmq_queue(channel, queue, prefetch_count=1):
    channel.basic_qos(global_qos=False, prefetch_count=prefetch_count)
    channel.queue_declare(
        queue=queue,
        durable=True,
        arguments={
            "x-max-priority": settings.RABBITMQ_MAX_PRIORITY,
            "x-overflow": "reject-publish",
        },
    )


class MaxRetriesExceededError(Exception):
    pass


class Worker(ABC):
    def __init__(
        self,
        queues,
        conn=None,
        num_threads=settings.WORKER_THREADS,
        version=None,
        prefetch_count_mapping=defaultdict(lambda: 1),
    ):
        if settings.SENTRY_DSN:
            import sentry_sdk

            sentry_sdk.init(
                dsn=settings.SENTRY_DSN,
                traces_sample_rate=0,
                release=settings.SENTRY_RELEASE,
                environment=settings.SENTRY_ENVIRONMENT,
                send_default_pii=False,
            )

        self.conn = conn or get_redis()
        self.num_threads = num_threads
        self.queues = ensure_list(queues)
        self.version = version
        self.local_queue = Queue()
        self.prefetch_count_mapping = prefetch_count_mapping
        if settings.SENTRY_DSN:
            import sentry_sdk

            sentry_sdk.init(
                dsn=settings.SENTRY_DSN,
                traces_sample_rate=0,
                release=settings.SENTRY_RELEASE,
                environment=settings.SENTRY_ENVIRONMENT,
                send_default_pii=False,
            )

    def run_prometheus_server(self):
        if not settings.PROMETHEUS_ENABLED:
            return

        def run_server():
            port = settings.PROMETHEUS_PORT
            log.info(f"Running Prometheus metrics server on port {port}")
            start_http_server(port)

        thread = Thread(target=run_server)
        thread.start()
        thread.join()

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
        # the task needs to be acknowledged in the same channel that it was
        # received. So store the channel. This is useful when executing batched
        # indexing tasks since they are acknowledged late.
        task._channel = channel
        self.local_queue.put((task, channel, connection))

    def process_blocking(self):
        """Blocking worker thread - executes tasks from a queue and periodic tasks"""
        while True:
            try:
                self.periodic()
                (task, channel, connection) = self.local_queue.get(timeout=TIMEOUT)
                apply_task_context(task, v=self.version)
                self.handle(task, channel)
                cb = functools.partial(self.ack_message, task, channel)
                connection.add_callback_threadsafe(cb)
            except Empty:
                pass
            finally:
                clear_contextvars()

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
                    self.handle(task, channel)

    def process(self, blocking=True):
        if blocking:
            self.process_blocking()
        else:
            self.process_nonblocking()

    def handle(self, task: Task, channel):
        """Execute a task."""
        try:
            dataset = Dataset(
                conn=self.conn, name=dataset_from_collection_id(task.collection_id)
            )
            if dataset.should_execute(task.task_id):
                task_retry_count = task.get_retry_count(self.conn)
                if task_retry_count:
                    metrics.TASKS_FAILED.labels(
                        stage=task.operation,
                        retries=task_retry_count,
                        failed_permanently=False,
                    ).inc()

                if task_retry_count > settings.WORKER_RETRY:
                    raise MaxRetriesExceededError(
                        f"Max retries reached for task {task.task_id}. Aborting."
                    )

                dataset.checkout_task(task.task_id, task.operation)
                task.increment_retry_count(self.conn)

                # Emit Prometheus metrics
                metrics.TASKS_STARTED.labels(stage=task.operation).inc()
                start_time = default_timer()
                log.info(
                    f"Dispatching task {task.task_id} from job {task.job_id}"
                    f"to worker {platform.node()}"
                )

                task = self.dispatch_task(task)

                # Emit Prometheus metrics
                duration = max(0, default_timer() - start_time)
                metrics.TASK_DURATION.labels(stage=task.operation).observe(duration)
                metrics.TASKS_SUCCEEDED.labels(
                    stage=task.operation, retries=task_retry_count
                ).inc()
            else:
                log.info(
                    f"Sending a NACK for message {task.delivery_tag}"
                    f" for task_id {task.task_id}."
                    f"Message will be requeued."
                )
                # In this case, a task ID was found neither in the
                # list of Pending, nor the list of Running tasks
                # in Redis. It was never attempted.
                metrics.TASKS_FAILED.labels(
                    stage=task.operation,
                    retries=0,
                    failed_permanently=True,
                ).inc()
                if channel.is_open:
                    channel.basic_nack(task.delivery_tag)
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
                f"Skipping acknowledging message"
                f"{task.delivery_tag} for task_id {task.task_id}"
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

        self.run_prometheus_server()

        # worker threads
        def process():
            return self.process(blocking=True)

        if not self.num_threads:
            # TODO - seems like we need at least one thread
            # consuming and processing require separate threads
            self.num_threads = 1

        threads = []
        for _ in range(self.num_threads):
            thread = threading.Thread(target=process)
            thread.daemon = True
            thread.start()
            threads.append(thread)

        log.info(f"Worker has {self.num_threads} worker threads.")

        connection = get_rabbitmq_connection()
        channel = connection.channel()
        on_message_callback = functools.partial(self.on_message, args=(connection,))

        for queue in self.queues:
            declare_rabbitmq_queue(
                channel, queue, prefetch_count=self.prefetch_count_mapping[queue]
            )
            channel.basic_consume(queue=queue, on_message_callback=on_message_callback)
        channel.start_consuming()


def get_rabbitmq_connection():
    for attempt in service_retries():
        try:
            if (
                not hasattr(local, "connection")
                or not local.connection
                or not local.connection.is_open
                or attempt > 0
            ):
                log.debug(
                    f"Establishing RabbitMQ connection. "
                    f"Attempt: {attempt}/{service_retries().stop}"
                )
                credentials = pika.PlainCredentials(
                    settings.RABBITMQ_USERNAME, settings.RABBITMQ_PASSWORD
                )
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=settings.RABBITMQ_URL,
                        credentials=credentials,
                        heartbeat=settings.RABBITMQ_HEARTBEAT,
                        blocked_connection_timeout=settings.RABBITMQ_BLOCKED_CONNECTION_TIMEOUT,
                        client_properties={"connection_name": f"{platform.node()}"},
                    )
                )
                local.connection = connection

            # Check that the connection is alive
            result = local.connection.channel().exchange_declare(
                exchange="amq.topic",
                exchange_type=pika.exchange_type.ExchangeType.topic,
                passive=True,
            )
            assert isinstance(result.method, pika.spec.Exchange.DeclareOk)

            return local.connection

        except (
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.AMQPError,
            pika.exceptions.ChannelClosedByBroker,
            pika.exceptions.StreamLostError,
            AssertionError,
            ConnectionResetError,
        ):
            if attempt == 0:
                log.debug(
                    "First attempt to establish RabbitMQ connection failed. Retrying."
                )
            else:
                log.exception(
                    f"Failed to establish RabbitMQ connection."
                    f"Attempt: {attempt}/{service_retries().stop}"
                )
        local.connection = None

        backoff(failures=attempt)
    raise RuntimeError("Could not connect to RabbitMQ")
