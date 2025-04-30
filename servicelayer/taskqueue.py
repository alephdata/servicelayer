from dataclasses import dataclass
from typing import Optional, Tuple, List
import json
import time
import threading
import signal
import faulthandler
import logging
import sys
from abc import ABC, abstractmethod
import functools
from queue import Queue, Empty, Full
import platform
from collections import defaultdict
from threading import Thread
from timeit import default_timer
import uuid
from random import randrange

import pika.spec
from pika.adapters.blocking_connection import BlockingChannel
from prometheus_client import start_http_server

from structlog.contextvars import clear_contextvars, bind_contextvars
import pika
from banal import ensure_list
from redis import Redis

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

    def clear_retry_count(self, conn):
        conn.delete(self.retry_key)

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
        self.last_update_key = make_key(PREFIX, "qdj", name, "last_update")
        self.active_stages_key = make_key(PREFIX, "qds", name, "active_stages")

    def flush_status(self, pipe):
        # remove the dataset from active datasets
        pipe.srem(self.key, self.name)

        # reset timestamps
        pipe.delete(self.start_key)
        pipe.delete(self.last_update_key)

        # delete information about running stages
        for stage in self.conn.smembers(self.active_stages_key):
            pipe.delete(make_key(PREFIX, "qds", self.name, stage))
            pipe.delete(make_key(PREFIX, "qds", self.name, stage, "pending"))
            pipe.delete(make_key(PREFIX, "qds", self.name, stage, "running"))
            pipe.delete(make_key(PREFIX, "qds", self.name, stage, "finished"))

        # delete information about tasks per dataset
        pipe.delete(self.pending_key)
        pipe.delete(self.running_key)
        pipe.delete(self.finished_key)

        # delete stages key
        pipe.delete(self.active_stages_key)

    def cancel(self):
        """Cancel processing of all tasks belonging to a dataset"""
        pipe = self.conn.pipeline()
        self.flush_status(pipe)
        pipe.execute()

    def get_status(self):
        """Status of a given dataset."""
        status = {"finished": 0, "running": 0, "pending": 0, "stages": []}

        start, last_update = self.conn.mget((self.start_key, self.last_update_key))
        status["start_time"] = start
        status["last_update"] = last_update

        for stage in self.conn.smembers(self.active_stages_key):
            num_pending = unpack_int(
                self.conn.scard(make_key(PREFIX, "qds", self.name, stage, "pending"))
            )
            num_running = unpack_int(
                self.conn.scard(make_key(PREFIX, "qds", self.name, stage, "running"))
            )
            num_finished = unpack_int(
                self.conn.get(make_key(PREFIX, "qds", self.name, stage, "finished"))
            )

            status["stages"].append(
                {
                    "job_id": "",
                    "stage": stage,
                    "pending": max(0, num_pending),
                    "running": max(0, num_running),
                    "finished": max(0, num_finished),
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
            if dataset.is_done():
                pipe = conn.pipeline()
                dataset.flush_status(pipe)
                pipe.execute()

    def should_execute(self, task_id):
        """Should a task be executed?

        When a the processing of a task is cancelled, there is no way
        to tell RabbitMQ to drop it. So we store that state in Redis
        and make the worker check with Redis before executing a task.
        """
        attempt = 1
        max_retries = 1
        while True:
            _should_execute = self.conn.sismember(
                self.pending_key, task_id
            ) or self.conn.sismember(self.running_key, task_id)
            if not _should_execute and attempt - 1 in range(max_retries):
                # Sometimes for new tasks the check fails because the Redis
                # state gets updated only after the task gets written to disk
                # by RabbitMQ whereas the worker consumer gets the task before
                # that.
                # Retry a few times to avoid those cases.
                backoff(
                    failures=attempt, reason="should_execute waiting for Redis task"
                )
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
        pipe.sadd(self.active_stages_key, stage)
        pipe.sadd(make_key(PREFIX, "qds", self.name, stage), task_id)
        pipe.sadd(make_key(PREFIX, "qds", self.name, stage, "pending"), task_id)

        # add the task to the set of pending tasks per dataset
        pipe.sadd(self.pending_key, task_id)

        # update dataset timestamps
        pipe.set(self.start_key, pack_now(), nx=True)
        pipe.set(self.last_update_key, pack_now())
        pipe.execute()

    def remove_task(self, task_id, stage):
        """Remove a task that's not going to be executed"""
        log.info(f"Removing task: {task_id}")
        pipe = self.conn.pipeline()

        # remove the task from the set of pending tasks per dataset
        pipe.srem(self.pending_key, task_id)

        pipe.srem(make_key(PREFIX, "qds", self.name, stage), task_id)
        pipe.srem(make_key(PREFIX, "qds", self.name, stage, "pending"), task_id)

        # delete the retry key for this task
        pipe.delete(make_key(PREFIX, "qdj", self.name, "taskretry", task_id))

        pipe.execute()

        if self.is_done():
            pipe = self.conn.pipeline()
            self.flush_status(pipe)
            pipe.execute()

    def checkout_task(self, task_id, stage):
        """Update state when a task is checked out for execution"""
        log.info(f"Checking out task: {task_id}")
        pipe = self.conn.pipeline()
        # add the dataset to active datasets
        pipe.sadd(self.key, self.name)

        # update status of stages per dataset
        pipe.sadd(self.active_stages_key, stage)
        pipe.sadd(make_key(PREFIX, "qds", self.name, stage), task_id)
        pipe.srem(make_key(PREFIX, "qds", self.name, stage, "pending"), task_id)
        pipe.sadd(make_key(PREFIX, "qds", self.name, stage, "running"), task_id)

        # add the task to the set of running tasks per dataset
        pipe.sadd(self.running_key, task_id)
        # remove the task from the set of pending tasks per dataset
        pipe.srem(self.pending_key, task_id)

        # update dataset timestamps
        pipe.set(self.start_key, pack_now(), nx=True)
        pipe.set(self.last_update_key, pack_now())
        pipe.execute()

    def mark_done(self, task: Task):
        """Update state when a task is finished executing"""
        log.info(f"Finished executing task: {task.task_id}")
        pipe = self.conn.pipeline()

        # remove the task from the pending and running sets of tasks per dataset
        pipe.srem(self.pending_key, task.task_id)
        pipe.srem(self.running_key, task.task_id)

        # increase the number of finished tasks per dataset
        pipe.incr(self.finished_key)

        # delete the retry key for the task
        pipe.delete(task.retry_key)

        stage = task.operation
        task_id = task.task_id
        pipe.srem(make_key(PREFIX, "qds", self.name, stage), task_id)
        pipe.srem(make_key(PREFIX, "qds", self.name, stage, "pending"), task_id)
        pipe.srem(make_key(PREFIX, "qds", self.name, stage, "running"), task_id)
        pipe.incr(make_key(PREFIX, "qds", self.name, stage, "finished"))

        # update dataset timestamps
        pipe.set(self.last_update_key, pack_now())

        pipe.execute()

        if self.is_done():
            pipe = self.conn.pipeline()
            self.flush_status(pipe)
            pipe.execute()

    def mark_for_retry(self, task):
        pipe = self.conn.pipeline()

        log.info(
            f"Marking task {task.task_id} (stage {task.operation})"
            f" for retry after NACK"
        )

        stage = task.operation
        task_id = task.task_id

        # remove the task from the pending and running sets of tasks per dataset
        pipe.srem(self.pending_key, task_id)
        pipe.srem(self.running_key, task_id)

        pipe.srem(make_key(PREFIX, "qds", self.name, stage, "pending"), task_id)
        pipe.srem(make_key(PREFIX, "qds", self.name, stage, "running"), task_id)
        pipe.srem(make_key(PREFIX, "qds", self.name, stage), task_id)

        pipe.set(self.last_update_key, pack_now())

        pipe.execute()

    def is_done(self):
        status = self.get_status()
        return status["pending"] == 0 and status["running"] == 0

    def __str__(self):
        return self.name

    def is_task_tracked(self, task: Task):
        tracked = True

        dataset = dataset_from_collection_id(task.collection_id)
        task_id = task.task_id
        stage = task.operation

        # A task is considered tracked if
        # the dataset is in the list of active datasets
        if not self.conn.sismember(self.key, dataset):
            tracked = False
        # and the stage is in the list of active stages
        elif not self.conn.sismember(self.active_stages_key, stage):
            tracked = False
        # and the task_id is in the list of task_ids per stage
        elif not self.conn.sismember(
            make_key(PREFIX, "qds", self.name, stage), task_id
        ):
            tracked = False

        return tracked

    @classmethod
    def is_low_prio(cls, conn, collection_id):
        """This Dataset is on the low prio list."""
        key = make_key(PREFIX, "prio", "low")
        return conn.sismember(key, collection_id)

    @classmethod
    def is_high_prio(cls, conn, collection_id):
        """This Dataset is on the high prio list."""
        key = make_key(PREFIX, "prio", "high")
        return conn.sismember(key, collection_id)


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
        conn: Redis = None,
        num_threads=settings.WORKER_THREADS,
        version=None,
        prefetch_count_mapping=defaultdict(lambda: 1),
    ):
        if settings.SENTRY_DSN:
            import sentry_sdk
            from sentry_sdk.integrations.threading import ThreadingIntegration

            sentry_sdk.init(
                dsn=settings.SENTRY_DSN,
                traces_sample_rate=0,
                release=settings.SENTRY_RELEASE,
                environment=settings.SENTRY_ENVIRONMENT,
                send_default_pii=False,
                disabled_integrations=[
                    ThreadingIntegration(),
                ],
            )
            log.info("Worker initialized Sentry SDK")

        self.conn = conn or get_redis()
        self.num_threads = num_threads
        self.queues = ensure_list(queues)
        self.version = version
        self.prefetch_count_mapping = prefetch_count_mapping

        # Limit the local queue size to the maximum prefetch count size
        max_queue_length = max(
            [self.prefetch_count_mapping[q] for q in self.queues], default=1
        )
        self.local_queue = Queue(maxsize=max_queue_length)
        log.info(f"local_queue initialized with maxsize={max_queue_length}")

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
        task = get_task(body, method.delivery_tag)
        # the task needs to be acknowledged in the same channel that it was
        # received. So store the channel. This is useful when executing batched
        # indexing tasks since they are acknowledged late.
        task._channel = channel
        try:
            self.local_queue.put_nowait((task, channel))
        except Full:
            channel.basic_nack(delivery_tag=task.delivery_tag, requeue=True)

    def process_blocking(self):
        """Blocking worker thread - executes tasks from a queue and periodic tasks"""
        while True:
            try:
                (task, channel) = self.local_queue.get(timeout=TIMEOUT)
                apply_task_context(task, v=self.version)
                success, retry = self.handle(task, channel)
                task_id = task.id if hasattr(task, "id") else "<Unknown>"
                log.debug(
                    f"Task {task_id} finished with success={success}"
                    f"{'' if success else ' and retry=' + str(retry)}"
                )
                if success:
                    cb = functools.partial(self.ack_message, task, channel)
                else:
                    cb = functools.partial(self.nack_message, task, channel, retry)
                channel.connection.add_callback_threadsafe(cb)
            except Empty:
                pass
            except Exception:
                collection_id = (
                    task.collection_id
                    if hasattr(task, "collection_id")
                    else "<Unknown>"
                )
                log.exception(
                    f"Worker loop has been disrupted while handling task"
                    f" {task_id} from collection {collection_id}"
                )
            finally:
                clear_contextvars()
                self.periodic()

    def process_nonblocking(self):
        """Non-blocking worker is used for tests only."""
        channel = get_rabbitmq_channel()
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
                    task._channel = channel
                    success, retry = self.handle(task, channel)
                    if success:
                        channel.basic_ack(task.delivery_tag)
                    else:
                        channel.basic_nack(task.delivery_tag, requeue=retry)

    def process(self, blocking=True):
        if blocking:
            self.process_blocking()
        else:
            self.process_nonblocking()

    def handle(self, task: Task, channel) -> Tuple[bool, bool]:
        """Execute a task.

        Returns a tuple of (success, retry)."""
        success = True
        retry = True

        task_retry_count = task.get_retry_count(self.conn)

        try:
            dataset = Dataset(
                conn=self.conn, name=dataset_from_collection_id(task.collection_id)
            )
            if dataset.should_execute(task.task_id):
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
                    f" Message will be requeued."
                )
                # In this case, a task ID was found neither in the
                # list of Pending, nor the list of Running tasks
                # in Redis. It was never attempted.
                success = False
        except MaxRetriesExceededError:
            log.exception(
                f"Task {task.task_id} permanently failed and will be discarded."
            )
            task.clear_retry_count(self.conn)
            success = False
            retry = False
        except Exception:
            log.exception("Error in task handling")
            metrics.TASKS_FAILED.labels(
                stage=task.operation,
                retries=task_retry_count,
                failed_permanently=task_retry_count >= settings.WORKER_RETRY,
            ).inc()
            success = False
        finally:
            self.after_task(task)
        return success, retry

    @abstractmethod
    def dispatch_task(self, task: Task) -> Task:
        raise NotImplementedError

    def after_task(self, task: Task):
        """Run after-task clean up"""
        pass

    def periodic(self):
        """Periodic tasks to run."""
        pass

    def ack_message(self, task, channel, multiple=False):
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
                channel.basic_ack(task.delivery_tag, multiple=multiple)
        clear_contextvars()

    def nack_message(self, task, channel, requeue=True):
        """NACK task and update status."""

        apply_task_context(task, v=self.version)
        log.info(f"NACKing message {task.delivery_tag} for task_id {task.task_id}")
        dataset = task.get_dataset(conn=self.conn)
        # Sync state to redis
        if requeue:
            dataset.mark_for_retry(task)
            if not dataset.is_task_tracked(task):
                dataset.add_task(task.task_id, task.operation)
        else:
            dataset.mark_done(task)
        if channel.is_open:
            channel.basic_nack(delivery_tag=task.delivery_tag, requeue=requeue)
        clear_contextvars()

    def run(self):
        """Run a blocking worker instance"""

        # Handle kill signals
        signal.signal(signal.SIGINT, self.on_signal)
        signal.signal(signal.SIGTERM, self.on_signal)
        faulthandler.register(signal.SIGUSR1)

        self.run_prometheus_server()

        # worker threads
        def process():
            return self.process(blocking=True)

        if not self.num_threads:
            # we need at least one thread
            # consuming and processing require separate threads
            self.num_threads = 1

        threads = []
        for _ in range(self.num_threads):
            thread = threading.Thread(target=process)
            thread.daemon = True
            thread.start()
            threads.append(thread)

        log.info(f"Worker has {self.num_threads} worker threads.")

        channel = get_rabbitmq_channel()
        on_message_callback = functools.partial(self.on_message, args=(channel,))

        for queue in self.queues:
            declare_rabbitmq_queue(
                channel, queue, prefetch_count=self.prefetch_count_mapping[queue]
            )
            channel.basic_consume(queue=queue, on_message_callback=on_message_callback)

        channel.start_consuming()


def get_rabbitmq_channel() -> BlockingChannel:
    backoff_reason = None
    for attempt in service_retries():
        try:
            if (
                not hasattr(local, "connection")
                or not local.connection
                or not local.connection.is_open
                or not local.channel
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
                local.channel = connection.channel()
                local.channel.confirm_delivery()

            # Check that the connection is alive
            result = local.channel.exchange_declare(
                exchange="amq.topic",
                exchange_type=pika.exchange_type.ExchangeType.topic,
                passive=True,
            )
            assert isinstance(result.method, pika.spec.Exchange.DeclareOk)

            return local.channel

        except (
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.AMQPError,
            pika.exceptions.ChannelClosedByBroker,
            pika.exceptions.StreamLostError,
            AssertionError,
            ConnectionResetError,
        ) as e:
            backoff_reason = repr(e)
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
        local.channel = None

        backoff(failures=attempt, reason=backoff_reason)
    raise RuntimeError("Could not connect to RabbitMQ")


def get_task_count(collection_id, redis_conn) -> int:
    """Get the total task count for a given dataset."""
    status = Dataset.get_active_dataset_status(conn=redis_conn)
    try:
        collection = status["datasets"][str(collection_id)]
        total = collection["finished"] + collection["running"] + collection["pending"]
    except KeyError:
        total = 0
    return total


def get_priority(collection_id, redis_conn) -> int:
    """
    Priority buckets for tasks based on the total (pending + running) task count.
    """
    if collection_id and Dataset.is_high_prio(redis_conn, collection_id):
        return 9
    if collection_id and Dataset.is_low_prio(redis_conn, collection_id):
        return 0
    total_task_count = get_task_count(collection_id, redis_conn)
    if total_task_count < 500:
        return randrange(7, 9)
    elif total_task_count < 10000:
        return randrange(4, 7)
    return randrange(1, 4)


def dataset_from_collection(collection):
    """servicelayer dataset from a collection"""
    if collection is None:
        return NO_COLLECTION
    return str(collection.id)


def queue_task(
    rmq_channel: BlockingChannel,
    redis_conn,
    collection_id: int,
    stage: str,
    job_id=None,
    context=None,
    **payload,
):
    task_id = uuid.uuid4().hex
    priority = get_priority(collection_id, redis_conn)
    body = {
        "collection_id": collection_id,
        "job_id": job_id or uuid.uuid4().hex,
        "task_id": task_id,
        "operation": stage,
        "context": context,
        "payload": payload,
        "priority": priority,
    }
    try:
        rmq_channel.basic_publish(
            exchange="",
            routing_key=stage,
            body=json.dumps(body),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE, priority=priority
            ),
        )
        dataset = Dataset(conn=redis_conn, name=str(collection_id))
        dataset.add_task(task_id, stage)
    except (pika.exceptions.UnroutableError, pika.exceptions.AMQPConnectionError):
        log.exception("Error while queuing task")


def flush_queues(rmq_channel: BlockingChannel, redis_conn: Redis, queues: List[str]):
    try:
        for queue in queues:
            try:
                rmq_channel.queue_purge(queue)
            except ValueError:
                logging.exception(f"Error while flushing the {queue} queue")
    except pika.exceptions.AMQPError:
        logging.exception("Error while flushing task queue")
    for key in redis_conn.scan_iter(PREFIX + "*"):
        redis_conn.delete(key)
