import time
import random
import logging
import uuid
from banal import ensure_list
from redis.exceptions import BusyLoadingError

from servicelayer.settings import REDIS_LONG
from servicelayer.cache import make_key
from servicelayer.util import pack_now, dump_json, load_json

log = logging.getLogger(__name__)
PREFIX = 'sla'


class Job(object):
    def __init__(self, conn, dataset, job_id):
        self.conn = conn
        self.dataset = dataset
        self.id = job_id

    @classmethod
    def random_id(cls):
        return uuid.uuid4().hex

    def is_done(self):
        status = Progress.get_job_status(self.conn, self.dataset, self.id)
        return status['pending'] < 1

    def remove(self):
        for operation in Progress.get_dataset_operations(self.conn, self.dataset):  # noqa
            for priority in JobOp.PRIORITIES:
                job_op = JobOp(
                    self.conn, operation, self.id,
                    self.dataset, priority=priority
                )
                job_op.remove()

    @classmethod
    def remove_dataset(cls, conn, dataset):
        """Delete all known queues associated with a dataset."""
        for job_id in Progress.get_dataset_job_ids(conn, dataset):
            job = cls(conn, dataset, job_id)
            job.remove()


class JobOp(object):
    OP_INGEST = 'ingest'
    OP_INDEX = 'index'
    OP_ANALYSIS = 'analysis'

    PRIO_HIGH = 3
    PRIO_MEDIUM = 2
    PRIO_LOW = 1
    PRIORITIES = [PRIO_HIGH, PRIO_MEDIUM, PRIO_LOW]

    def __init__(self, conn, operation, job_id, dataset, priority=PRIO_MEDIUM):  # noqa
        self.conn = conn
        self.operation = operation
        self.job = Job(conn, dataset, job_id)
        self.dataset = dataset
        self.priority = priority
        self.progress = Progress(conn, operation, self.job.id, dataset)
        self.queue_key = make_key(PREFIX, 'q', operation, self.job.id, dataset)
        self.ops_queue_key = make_key(PREFIX, 'qo', operation, priority)

    def queue_task(self, payload, context):
        self.conn.sadd(self.ops_queue_key, self.queue_key)
        data = dump_json({
            'context': context or {},
            'payload': payload,
            'dataset': self.dataset,
            'job_id': self.job.id,
            'operation': self.operation,
            'priority': self.priority
        })
        self.conn.rpush(self.queue_key, data)
        self.progress.mark_pending()

    def task_done(self):
        self.progress.mark_finished()
        # Remove job if done
        if self.job.is_done():
            self.job.remove()

    def is_done(self):
        """Are the tasks for the current `job_id` and `operation` done?"""
        status = self.progress.get()
        return status.get('pending') < 1

    def remove(self):
        """Remove tasks for the current `job_id` and `operation`"""
        self.conn.srem(self.ops_queue_key, self.queue_key)
        self.conn.delete(self.queue_key)
        self.progress.remove()

    def is_job_done(self):
        """Are all the tasks for `job_id` done?"""
        return self.job.is_done()

    def remove_job(self):
        """Remove all tasks for `job_id`"""
        return self.job.remove()

    def get_tasks(self, limit=500):
        assert limit > 0
        pipe = self.conn.pipeline()
        pipe.lrange(self.queue_key, 0, limit-1)
        pipe.ltrim(self.queue_key, limit, -1)
        tasks = pipe.execute()[0]
        for task in tasks:
            task = load_json(task)
            operation = task.get('operation')
            job_id = task.get('job_id')
            dataset = task.get('dataset')
            priority = task.get('priority')
            job_op = self.__class__(
                self.conn, operation, job_id, dataset, priority=priority
            )
            yield (job_op, task.get('payload'), task.get('context'))

    @classmethod
    def _get_operation_queues(cls, conn, operations):
        """Return all the active queues for the given operation."""
        queues = []
        for priority in cls.PRIORITIES:
            prio_queues = []
            for op in ensure_list(operations):
                key = make_key(PREFIX, 'qo', op, priority)
                prio_queues.extend(conn.smembers(key))
            # TODO: do we want to random.shuffle?
            random.shuffle(prio_queues)
            for queue in prio_queues:
                if queue not in queues:
                    queues.append(queue)
        return queues

    @classmethod
    def get_operation_task(cls, conn, operations, timeout=0):
        """Retrieve a single task from the highest-priority queue that has
        work pending."""
        try:
            queues = cls._get_operation_queues(conn, operations)
            if not len(queues):
                return (None, None, None)
            # Support a magic value to not block, i.e. timeout None
            if timeout is None:
                # LPOP does not support multiple lists.
                for queue in queues:
                    task_data = conn.lpop(queue)
                    if task_data is not None:
                        break
            else:
                task_data = conn.blpop(queues, timeout=timeout)
                if task_data is not None:
                    key, task_data = task_data

            if task_data is None:
                return (None, None, None)

            task = load_json(task_data)
            operation = task.get('operation')
            job_id = task.get('job_id')
            dataset = task.get('dataset')
            priority = task.get('priority')
            job_op = cls(conn, operation, job_id, dataset, priority=priority)
            return (job_op, task.get('payload'), task.get('context'))
        except BusyLoadingError:
            time.sleep(timeout + 1)
            return (None, None, None)


class Progress(object):
    """An abstraction for the notion of a process that covers a set
    of items. This is simplified and does not cover the notion of a
    task being in-progress."""
    PENDING = 'pending'
    FINISHED = 'finished'
    # TODO: do we need a 'running' state?

    def __init__(self, conn, operation, job_id, dataset):
        _key = make_key(PREFIX, 'p', operation, job_id, dataset)
        self.conn = conn
        self.operation = operation
        self.job_id = job_id
        self.dataset = dataset
        self.dataset_key = make_key(PREFIX, 'qd', dataset)
        self.dataset_jobs_key = make_key(PREFIX, 'qdj', dataset)
        self.pending_key = make_key(_key, self.PENDING)
        self.finished_key = make_key(_key, self.FINISHED)

    def mark_active(self):
        """Add the dataset to the list of datasets that are running. Add the
        job and it's start time if it doesn't exist already."""
        self.conn.sadd(self.dataset_key, self.operation)
        if not self.conn.hget(self.dataset_jobs_key, self.job_id):
            self.conn.hset(self.dataset_jobs_key, self.job_id, pack_now())

    def mark_pending(self, amount=1):
        self.mark_active()
        self.conn.incr(self.pending_key, amount=amount)

    def mark_finished(self, amount=1):
        """Move items from the pending to the finished set."""
        pipe = self.conn.pipeline()
        pipe.decr(self.pending_key, amount=amount)
        pipe.incr(self.finished_key, amount=amount)
        pipe.execute()

    def put_finished(self, amount=1):
        """Add a number of items to the finished set without reducing
        the count of pending items."""
        self.mark_active()
        self.conn.incr(self.finished_key, amount=amount)

    def remove(self):
        pipe = self.conn.pipeline()
        pipe.srem(self.dataset_key, self.operation)
        pipe.expire(self.dataset_key, REDIS_LONG)
        pipe.hdel(self.dataset_jobs_key, self.job_id)
        pipe.delete(self.pending_key, self.finished_key)
        pipe.execute()

    def get(self):
        """Get the current status."""
        keys = (self.pending_key, self.finished_key)
        pending, finished = self.conn.mget(keys)
        return {
            'job_id': self.job_id,
            'operation': self.operation,
            'pending': max(0, int(pending or 0)),
            'finished': max(0, int(finished or 0)),
        }

    @classmethod
    def get_dataset_operations(cls, conn, dataset):
        return conn.smembers(make_key(PREFIX, 'qd', dataset))

    @classmethod
    def get_dataset_job_ids(cls, conn, dataset):
        return conn.hkeys(make_key(PREFIX, 'qdj', dataset))

    @classmethod
    def get_job_status(cls, conn, dataset, job_id):
        """Aggregate status for all operations on the given job."""
        status = {'finished': 0, 'pending': 0, 'operations': []}
        status['start_time'] = conn.hget(make_key(PREFIX, 'qdj', dataset), job_id)  # noqa
        for operation in cls.get_dataset_operations(conn, dataset):
            progress = cls(conn, operation, job_id, dataset).get()
            status['operations'].append(progress)
            status['finished'] += progress['finished']
            status['pending'] += progress['pending']
        return status

    @classmethod
    def get_dataset_status(cls, conn, dataset):
        """Aggregate status for all operations on the given dataset."""
        status = {'finished': 0, 'pending': 0, 'jobs': []}
        for job_id in cls.get_dataset_job_ids(conn, dataset):
            progress = cls.get_job_status(conn, dataset, job_id)
            status['jobs'].append(progress)
            status['finished'] += progress['finished']
            status['pending'] += progress['pending']
        return status


class RateLimit(object):
    """Limit the rate of operations on a given resource during a
    stated interval."""

    def __init__(self, conn, resource, limit=100, interval=60, unit=1):
        self.conn = conn
        self.resource = resource
        self.limit = max(0.1, limit)
        self.interval = max(1, interval)
        self.unit = unit

    def _time(self):
        return int(time.time() / self.unit)

    def _keys(self):
        base = self._time()
        for slot in range(base, base + self.interval):
            yield make_key(PREFIX, 'rate', self.resource, slot)

    def update(self, amount=1):
        """Set the cached counts for stats keeping."""
        pipe = self.conn.pipeline()
        for key in self._keys():
            pipe.incr(key, amount=amount)
            pipe.expire(key, (self.interval * self.unit) + 2)
        values = pipe.execute()[::2]
        return (sum(values) / self.interval)

    def check(self):
        """Check if the resource has exceeded the rate limit."""
        key = make_key(PREFIX, 'rate', self.resource, self._time())
        count = int(self.conn.get(key) or 0)
        return count < self.limit

    def comply(self, amount=1):
        """Update, then sleep for the time required to adhere to the
        rate limit."""
        # FIXME: this is trying to be smart, which will probably
        # backfire. Rather than just halt when the rate is exceeded,
        # this is trying to average down calls to the appropriate
        # frequency.
        rate = self.update(amount=amount) / self.limit
        expected = self.interval / self.limit
        excess = rate - expected
        if excess > 0:
            time.sleep(excess)
        return rate
