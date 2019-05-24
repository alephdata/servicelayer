import json
import time
import random
import logging
from banal import ensure_list

# from servicelayer.settings import REDIS_EXPIRE
from servicelayer.cache import make_key

log = logging.getLogger(__name__)
PREFIX = 'sla'


class ServiceQueue(object):
    OP_INGEST = 'ingest'
    OP_INDEX = 'index'
    OP_ANALYSIS = 'analysis'

    PRIO_HIGH = 3
    PRIO_MEDIUM = 2
    PRIO_LOW = 1
    PRIORITIES = [PRIO_HIGH, PRIO_MEDIUM, PRIO_LOW]

    def __init__(self, conn, operation, dataset, priority=PRIO_MEDIUM):
        self.conn = conn
        self.operation = operation
        self.dataset = dataset
        self.priority = priority
        self.progress = Progress(conn, operation, dataset)
        self.queue_key = make_key(PREFIX, 'q', operation, dataset)
        self.ops_queue_key = make_key(PREFIX, 'qo', operation, priority)

    def queue_task(self, payload, context):
        self.conn.sadd(self.ops_queue_key, self.queue_key)
        data = json.dumps({
            'context': context or {},
            'payload': payload,
            'dataset': self.dataset,
            'operation': self.operation,
            'priority': self.priority
        })
        self.conn.rpush(self.queue_key, data)
        self.progress.mark_pending()

    def task_done(self):
        self.progress.mark_finished()

    def is_done(self):
        status = self.progress.get()
        return status.get('pending') < 1

    def remove(self):
        self.conn.sadd(self.ops_queue_key, self.queue_key)
        self.conn.delete(self.queue_key)
        self.progress.remove()

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
        queues = cls._get_operation_queues(conn, operations)
        if not len(queues):
            return (None, None, None)
        task_data_tuple = conn.blpop(queues, timeout=timeout)
        # blpop blocks until it finds something. But fakeredis has no
        # blocking support. So it justs returns None.
        if task_data_tuple is None:
            return (None, None, None)

        key, json_data = task_data_tuple
        task = json.loads(json_data)
        operation = task.get('operation')
        dataset = task.get('dataset')
        queue = cls(conn, operation, dataset, priority=task.get('priority'))
        return (queue, task.get('payload'), task.get('context'))

    @classmethod
    def remove_dataset(cls, conn, dataset):
        """Delete all known queues associated with a dataset."""
        for operation in Progress.get_dataset_operations(conn, dataset):
            for priority in cls.PRIORITIES:
                queue = cls(conn, operation, dataset, priority=priority)
                queue.remove()


class Progress(object):
    """An abstraction for the notion of a process that covers a set
    of items. This is simplified and does not cover the notion of a
    task being in-progress."""
    PENDING = 'pending'
    FINISHED = 'finished'
    # TODO: do we need a 'running' state?

    def __init__(self, conn, operation, dataset):
        _key = make_key(PREFIX, 'p', operation, dataset)
        self.conn = conn
        self.operation = operation
        self.dataset = dataset
        self.dataset_key = make_key(PREFIX, 'qd', dataset)
        self.pending_key = make_key(_key, self.PENDING)
        self.finished_key = make_key(_key, self.FINISHED)

    def mark_active(self):
        """Add the dataset to the list of datasets that are
        running."""
        self.conn.sadd(self.dataset_key, self.operation)

    def mark_pending(self, amount=1):
        self.mark_active()
        self.conn.incr(self.pending_key, amount=amount)

    def mark_finished(self, amount=1):
        """Move items from the pending to the finished set."""
        self.conn.decr(self.pending_key, amount=amount)
        self.conn.incr(self.finished_key, amount=amount)

    def put_finished(self, amount=1):
        """Add a number of items to the finished set without reducing
        the count of pending items."""
        self.mark_active()
        self.conn.incr(self.finished_key, amount=amount)

    def remove(self):
        self.conn.srem(self.dataset_key, self.operation)
        self.conn.delete(self.pending_key, self.finished_key)

    def get(self):
        """Get the current status."""
        keys = (self.pending_key, self.finished_key)
        pending, finished = self.conn.mget(keys)
        return {
            'operation': self.operation,
            'pending': max(0, int(pending or 0)),
            'finished': max(0, int(finished or 0)),
        }

    @classmethod
    def get_dataset_operations(cls, conn, dataset):
        return conn.smembers(make_key(PREFIX, 'qd', dataset))

    @classmethod
    def get_dataset_status(cls, conn, dataset):
        """Aggregate status for all operations on the given dataset."""
        status = {'finished': 0, 'pending': 0, 'operations': []}
        for operation in cls.get_dataset_operations(conn, dataset):
            progress = cls(conn, operation, dataset).get()
            status['operations'].append(progress)
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
