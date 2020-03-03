import time
import random
import logging
import uuid
from banal import ensure_list

from redis.exceptions import BusyLoadingError

from servicelayer.settings import REDIS_EXPIRE
from servicelayer.settings import REDIS_PREFIX as PREFIX
from servicelayer.cache import make_key
from servicelayer.util import pack_now, dump_json, load_json
from servicelayer.util import unpack_int, sum_values

log = logging.getLogger(__name__)


class Dataset(object):
    """A subdivision of tasks. Each dataset can have multiple stages
    of processing and jobs that control processing instances."""

    def __init__(self, conn, name):
        self.conn = conn
        self.name = name
        self.key = make_key(PREFIX, 'qdatasets')
        self.jobs_key = make_key(PREFIX, 'qdj', name)
        self.stages_key = make_key(PREFIX, 'qds', name)

    def cancel(self):
        pipe = self.conn.pipeline()
        for job in self.get_jobs():
            job._remove(pipe)
        pipe.delete(self.stages_key)
        pipe.delete(self.jobs_key)
        pipe.execute()

    def get_stages(self):
        return self.conn.smembers(self.stages_key)

    def get_job_ids(self):
        return self.conn.smembers(self.jobs_key)

    def get_jobs(self):
        for job_id in self.get_job_ids():
            yield Job(self.conn, self, job_id)

    def get_status(self):
        """Aggregate status for all stages on the given dataset."""
        status = {'finished': 0, 'running': 0, 'pending': 0, 'jobs': []}
        for job in self.get_jobs():
            progress = job.get_status()
            status['jobs'].append(progress)
            status['finished'] += progress['finished']
            status['running'] += progress['running']
            status['pending'] += progress['pending']
        return status

    def __str__(self):
        return self.name

    @classmethod
    def ensure(cls, conn, name):
        if isinstance(name, cls):
            return name
        return cls(conn, name)

    @classmethod
    def get_active_datasets(cls, conn):
        datasets_key = make_key(PREFIX, 'qdatasets')
        for name in conn.smembers(datasets_key):
            yield cls(conn, name)

    @classmethod
    def get_active_dataset_status(cls, conn):
        result = {'total': 0, 'datasets': {}}
        for dataset in cls.get_active_datasets(conn):
            status = dataset.get_status()
            result['total'] += 1
            result['datasets'][dataset.name] = status
        return result


class Job(object):

    def __init__(self, conn, dataset, job_id):  # noqa
        self.conn = conn
        self.id = job_id
        self.dataset = Dataset.ensure(conn, dataset)
        self.start_key = make_key(PREFIX, 'qd', self.id, dataset, 'start')
        self.end_key = make_key(PREFIX, 'qd', self.id, dataset, 'end')
        self.active_jobs_key = make_key(PREFIX, 'qdja')

    def get_stage(self, name):
        return Stage(self, name)

    def get_stages(self):
        for stage in self.dataset.get_stages():
            yield self.get_stage(stage)

    def is_done(self):
        if self.conn.exists(self.end_key):
            return True
        for _ in range(5):
            keys = self._get_active_keys()
            active = sum_values(self.conn.mget(keys))
            if active > 0:
                return False

            for stage in self.get_stages():
                pending = self.conn.llen(stage.queue_key)
                self.conn.set(stage.pending_key, pending)
        self.conn.setnx(self.end_key, pack_now())
        return True

    def _create(self, pipe):
        pipe.sadd(self.dataset.key, self.dataset.name)
        pipe.sadd(self.dataset.jobs_key, self.id)
        pipe.sadd(self.active_jobs_key, make_key(self.dataset.name, self.id))
        pipe.delete(self.end_key)
        pipe.setnx(self.start_key, pack_now())

    def _remove(self, pipe):
        for stage in self.get_stages():
            stage._remove(pipe)
        pipe.srem(self.dataset.key, self.dataset.name)
        pipe.srem(self.dataset.jobs_key, self.id)
        pipe.srem(self.active_jobs_key, make_key(self.dataset.name, self.id))
        pipe.delete(self.start_key)
        pipe.setnx(self.end_key, pack_now())
        pipe.expire(self.end_key, REDIS_EXPIRE)

    def remove(self):
        pipe = self.conn.pipeline()
        self._remove(pipe)
        pipe.execute()

    def _get_active_keys(self):
        """Return the pending keys for all stages in this job"""
        keys = []
        for stage in self.get_stages():
            keys.append(stage.pending_key)
            keys.append(stage.running_key)
        return keys

    def get_status(self):
        """Aggregate status for all stages on the given job."""
        status = {'finished': 0, 'running': 0, 'pending': 0, 'stages': []}
        start, end = self.conn.mget((self.start_key, self.end_key))
        status['start_time'] = start
        status['end_time'] = end
        for stage in self.get_stages():
            progress = stage.get_status()
            status['stages'].append(progress)
            status['finished'] += progress['finished']
            status['running'] += progress['running']
            status['pending'] += progress['pending']
        return status

    @classmethod
    def random_id(cls):
        return uuid.uuid4().hex

    @classmethod
    def create(cls, conn, dataset):
        return cls(conn, dataset=dataset, job_id=Job.random_id())


class Stage(object):

    def __init__(self, job, stage):  # noqa
        self.job = job
        self.conn = job.conn
        self.stage = stage
        self.queue_key = make_key(PREFIX, 'q', job.dataset, stage, job.id)
        self.stages_key = self._get_stage_jobs_key(stage)
        self.pending_key = make_key(self.queue_key, 'pending')
        self.running_key = make_key(self.queue_key, 'running')
        self.finished_key = make_key(self.queue_key, 'finished')

    def _create(self, pipe):
        pipe.sadd(self.stages_key, self.queue_key)
        pipe.sadd(self.job.dataset.stages_key, self.stage)
        self.job._create(pipe)

    def _remove(self, pipe):
        """Remove tasks for the current `job_id` and `stage`"""
        pipe.srem(self.stages_key, self.queue_key)
        pipe.delete(self.queue_key, self.pending_key,
                    self.running_key, self.finished_key)

    def _check_out(self, count=1):
        """Check out tasks and mark them as running."""
        pipe = self.conn.pipeline()
        self._create(pipe)
        pipe.decr(self.pending_key, amount=count)
        pipe.incr(self.running_key, amount=count)
        pipe.execute()

    def mark_done(self, count=1):
        """Returns tasks previously checked out and mark as done."""
        pipe = self.conn.pipeline()
        self._create(pipe)
        pipe.decr(self.running_key, amount=count)
        pipe.incr(self.finished_key, amount=count)
        pipe.execute()

    def report_finished(self, count=1):
        """Inflate the number of finished tasks."""
        pipe = self.conn.pipeline()
        self._create(pipe)
        pipe.incr(self.finished_key, amount=count)
        pipe.execute()

    def queue(self, payload={}, context={}):
        task = Task(self, payload, context)
        data = task.serialize()
        pipe = self.conn.pipeline()
        self._create(pipe)
        pipe.rpush(self.queue_key, data)
        pipe.incr(self.pending_key)
        pipe.execute()
        return task

    def sync(self):
        pending = self.conn.llen(self.queue_key)
        self.conn.set(self.pending_key, pending)

    def get_tasks(self, limit=100):
        """Get multiple tasks at once, without blocking. This is used
        inside the consumer applications to process multiple tasks of
        the same type at once."""
        if limit is None or limit < 1:
            return []
        pipe = self.conn.pipeline()
        pipe.lrange(self.queue_key, 0, limit - 1)
        pipe.ltrim(self.queue_key, limit, -1)
        raw_tasks = pipe.execute()[0]
        tasks = []
        for task in raw_tasks:
            task = Task.unpack(self.conn, task)
            tasks.append(task)
        # TODO: can this be atomic?
        self._check_out(len(tasks))
        return tasks

    def get_status(self):
        """Get the current status."""
        keys = (self.pending_key, self.running_key, self.finished_key)
        pending, running, finished = self.conn.mget(keys)
        return {
            'job_id': self.job.id,
            'stage': self.stage,
            'pending': max(0, unpack_int(pending)),
            'running': max(0, unpack_int(running)),
            'finished': max(0, unpack_int(finished)),
        }

    @classmethod
    def _get_stage_jobs_key(cls, stage):
        return make_key(PREFIX, 'qos', stage)

    @classmethod
    def _get_queues(cls, conn, stages):
        """Return all the active queues for the given stage."""
        queues = []
        for stage in ensure_list(stages):
            key = cls._get_stage_jobs_key(stage)
            queues.extend(conn.smembers(key))
        # TODO: do we want to random.shuffle?
        random.shuffle(queues)
        return queues

    @classmethod
    def get_task(cls, conn, stages, timeout=0):
        """Retrieve a single task from the highest-priority queue that has
        work pending."""
        try:
            queues = cls._get_queues(conn, stages)
            if not len(queues):
                # Avoid going into a tight loop when there are no tasks:
                # cf. https://github.com/alephdata/aleph/issues/867
                if timeout is not None:
                    time.sleep(max(10, timeout or 1))
                return None
            # Support a magic value to not block, i.e. timeout None
            if timeout is None:
                # LPOP does not support multiple lists.
                for queue in queues:
                    task_data = conn.lpop(queue)
                    if task_data is not None:
                        break
                if task_data is None:
                    return None
            else:
                task_data = conn.blpop(queues, timeout=timeout)
                if task_data is None:
                    return None
                _, task_data = task_data

            task = Task.unpack(conn, task_data)
            # TODO: can this be atomic?
            task.stage._check_out(1)
            return task
        except BusyLoadingError:
            time.sleep(timeout + 1)
            return None


class Task(object):
    def __init__(self, stage, payload, context):
        self.payload = payload
        self.context = context
        self.stage = stage
        self.job = stage.job

    def done(self):
        self.stage.mark_done(1)

    def serialize(self):
        return dump_json({
            'context': self.context or {},
            'payload': self.payload,
            'dataset': self.job.dataset.name,
            'job': self.job.id,
            'stage': self.stage.stage
        })

    @classmethod
    def unpack(cls, conn, data):
        if data is None:
            return None
        data = load_json(data)
        job = Job(conn, data.get('dataset'), data.get('job'))
        stage = job.get_stage(data.get('stage'))
        return Task(stage, data.get('payload'), data.get('context'))
