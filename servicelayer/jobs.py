import time
import random
import logging
import uuid
from banal import ensure_list

from redis.exceptions import BusyLoadingError, WatchError

from servicelayer.settings import REDIS_LONG
from servicelayer.settings import REDIS_PREFIX as PREFIX
from servicelayer.cache import make_key
from servicelayer.util import pack_now, dump_json, load_json

log = logging.getLogger(__name__)


class Job(object):
    def __init__(self, conn, dataset, job_id):
        self.conn = conn
        self.dataset = dataset
        self.id = job_id
        self.executing_tasks_key = make_key(PREFIX, 'qjt', self.id, dataset)
        self.callback = None

    @classmethod
    def random_id(cls):
        return uuid.uuid4().hex

    def is_done(self):
        status = Progress.get_job_status(self.conn, self.dataset, self.id)
        return status['pending'] < 1

    def remove(self):
        for stage in Progress.get_dataset_stages(self.conn, self.dataset):  # noqa
            for priority in JobStage.PRIORITIES:
                job_stage = JobStage(
                    self.conn, stage, self.id,
                    self.dataset, priority=priority
                )
                job_stage.remove()

    def _get_all_pending_keys(self):
        """Return the pending keys for all stages in this job"""
        keys = []
        for stage in Progress.get_dataset_stages(self.conn, self.dataset):
            for priority in JobStage.PRIORITIES:
                job_stage = JobStage(
                    self.conn, stage, self.id,
                    self.dataset, priority=priority
                )
                keys.append(job_stage.progress.pending_key)
        return keys

    def execute_if_done(self, callback, *args, **kwargs):
        pipe = self.conn.pipeline()
        try:
            pipe.watch(self.executing_tasks_key, *self._get_all_pending_keys())
            # Make sure there is no tasks currently being executed
            if not self.conn.llen(self.executing_tasks_key) == 0:
                return
            # Make sure none of the stages for the job has non-zero pending
            # tasks
            pending_list = [
                max(0, int(pending or 0)) for pending in pipe.mget(
                    self._get_all_pending_keys()
                )
            ]
            if not all(val == 0 for val in pending_list):
                return
            callback(*args, **kwargs)
        except WatchError:
            log.info("State changed. Not executing callback for job %s", self.id)  # noqa
            return

    def remove_executing_task(self, task):
        self.conn.lrem(self.executing_tasks_key, 1, task.task_id)

    def add_executing_task(self, task):
        self.conn.rpush(self.executing_tasks_key, task.task_id)

    @classmethod
    def remove_dataset(cls, conn, dataset):
        """Delete all known queues associated with a dataset."""
        for job_id in Progress.get_dataset_job_ids(conn, dataset):
            job = cls(conn, dataset, job_id)
            job.remove()


class Task(object):
    def __init__(self, stage, payload=None, context=None, task_id=None):
        self.payload = payload
        self.context = context
        self.stage = stage
        self.job = stage.job
        self.task_id = task_id or self._make_task_id()

    def serialize(self):
        return dump_json({
            'context': self.context or {},
            'payload': self.payload,
            'dataset': self.stage.dataset,
            'job_id': self.job.id,
            'task_id': self.task_id,
            'stage': self.stage.stage,
            'priority': self.stage.priority
        })

    def queue(self):
        self.stage.queue_task(self)

    def done(self):
        self.stage.task_done(self)

    def _make_task_id(self):
        return uuid.uuid4().hex

    @classmethod
    def unpack_taskdata(cls, conn, task_data):
        if task_data is None:
            return None
        task_data = load_json(task_data)
        stage = task_data.get('stage')
        job_id = task_data.get('job_id')
        dataset = task_data.get('dataset')
        priority = task_data.get('priority')
        job_stage = JobStage(conn, stage, job_id, dataset, priority=priority)
        payload = task_data.get('payload')
        context = task_data.get('context')
        task_id = task_data.get('task_id')
        task = Task(job_stage, payload=payload, context=context, task_id=task_id)  # noqa
        # Add task to the list of currently executing tasks
        job = Job(conn, dataset, job_id)
        job.add_executing_task(task)
        return task


class JobStage(object):
    INGEST = 'ingest'

    PRIO_HIGH = 3
    PRIO_MEDIUM = 2
    PRIO_LOW = 1
    PRIORITIES = [PRIO_HIGH, PRIO_MEDIUM, PRIO_LOW]

    def __init__(self, conn, stage, job_id, dataset, priority=PRIO_MEDIUM):  # noqa
        self.conn = conn
        self.stage = stage
        self.job = Job(conn, dataset, job_id)
        self.dataset = dataset
        self.priority = priority
        self.progress = Progress(conn, stage, self.job.id, dataset)
        self.queue_key = make_key(PREFIX, 'q', stage, self.job.id, dataset)
        self.ops_queue_key = make_key(PREFIX, 'qo', stage, priority)

    def queue_task(self, task):
        self.conn.sadd(self.ops_queue_key, self.queue_key)
        data = task.serialize()
        self.conn.rpush(self.queue_key, data)
        self.progress.mark_pending()

    def task_done(self, task):
        self.job.remove_executing_task(task)
        self.progress.mark_finished()
        self.sync()

    def _sync(self, pipe):
        pending = pipe.llen(self.queue_key)
        pipe.set(self.progress.pending_key, pending)

    def sync(self):
        self.conn.transaction(self._sync, self.queue_key)

    def is_done(self):
        """Are the tasks for the current `job_id` and `stage` done?"""
        status = self.progress.get()
        return status.get('pending') < 1

    def remove(self):
        """Remove tasks for the current `job_id` and `stage`"""
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
            yield Task.unpack_taskdata(self.conn, task)

    @classmethod
    def _get_stage_queues(cls, conn, stages):
        """Return all the active queues for the given stage."""
        queues = []
        for priority in cls.PRIORITIES:
            prio_queues = []
            for op in ensure_list(stages):
                key = make_key(PREFIX, 'qo', op, priority)
                prio_queues.extend(conn.smembers(key))
            # TODO: do we want to random.shuffle?
            random.shuffle(prio_queues)
            for queue in prio_queues:
                if queue not in queues:
                    queues.append(queue)
        return queues

    @classmethod
    def get_stage_task(cls, conn, stages, timeout=0):
        """Retrieve a single task from the highest-priority queue that has
        work pending."""
        try:
            queues = cls._get_stage_queues(conn, stages)
            if not len(queues):
                return None
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

            return Task.unpack_taskdata(conn, task_data)
        except BusyLoadingError:
            time.sleep(timeout + 1)
            return None


class Progress(object):
    """An abstraction for the notion of a process that covers a set
    of items. This is simplified and does not cover the notion of a
    task being in-progress."""
    PENDING = 'pending'
    FINISHED = 'finished'
    # TODO: do we need a 'running' state?

    def __init__(self, conn, stage, job_id, dataset):
        _key = make_key(PREFIX, 'p', stage, job_id, dataset)
        self.conn = conn
        self.stage = stage
        self.job_id = job_id
        self.dataset = dataset
        self.dataset_key = make_key(PREFIX, 'qd', dataset)
        self.dataset_jobs_key = make_key(PREFIX, 'qdj', dataset)
        self.pending_key = make_key(_key, self.PENDING)
        self.finished_key = make_key(_key, self.FINISHED)

    def mark_active(self):
        """Add the dataset to the list of datasets that are running. Add the
        job and it's start time if it doesn't exist already."""
        self.conn.sadd(self.dataset_key, self.stage)
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
        pipe.srem(self.dataset_key, self.stage)
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
            'stage': self.stage,
            'pending': max(0, int(pending or 0)),
            'finished': max(0, int(finished or 0)),
        }

    @classmethod
    def get_dataset_stages(cls, conn, dataset):
        return conn.smembers(make_key(PREFIX, 'qd', dataset))

    @classmethod
    def get_dataset_job_ids(cls, conn, dataset):
        return conn.hkeys(make_key(PREFIX, 'qdj', dataset))

    @classmethod
    def get_job_status(cls, conn, dataset, job_id):
        """Aggregate status for all stages on the given job."""
        status = {'finished': 0, 'pending': 0, 'stages': []}
        status['start_time'] = conn.hget(make_key(PREFIX, 'qdj', dataset), job_id)  # noqa
        for stage in cls.get_dataset_stages(conn, dataset):
            progress = cls(conn, stage, job_id, dataset).get()
            status['stages'].append(progress)
            status['finished'] += progress['finished']
            status['pending'] += progress['pending']
        return status

    @classmethod
    def get_dataset_status(cls, conn, dataset):
        """Aggregate status for all stages on the given dataset."""
        status = {'finished': 0, 'pending': 0, 'jobs': []}
        for job_id in cls.get_dataset_job_ids(conn, dataset):
            progress = cls.get_job_status(conn, dataset, job_id)
            status['jobs'].append(progress)
            status['finished'] += progress['finished']
            status['pending'] += progress['pending']
        return status


class RateLimit(object):
    """Limit the rate of stages on a given resource during a
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
