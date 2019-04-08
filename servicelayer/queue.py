import json

from servicelayer import settings
from servicelayer.cache import get_redis, make_key

TASK_PENDING = 'pending'
TASK_RUNNING = 'executing'
TASK_FINISHED = 'finished'
TASK_TOTAL = 'total'


def _serialize(data):
    return json.dumps(data)


def _deserialize(data):
    return json.loads(data)


def push_task(queue, dataset, entity, config):
    assert queue in settings.QUEUES
    conn = get_redis()
    conn.rpush(make_key('ingest', 'queue', queue), _serialize({
        'dataset': dataset,
        'entity': entity,
        'config': config,
    }))
    conn.incr(make_key('ingest', TASK_PENDING, dataset))


def poll_task():
    conn = get_redis()
    queues = [make_key('ingest', 'queue', q) for q in settings.QUEUES]
    while True:
        task_data_tuple = conn.blpop(queues)
        # blpop blocks until it finds something. But fakeredis has no
        # blocking support. So it justs returns None.
        if task_data_tuple is None:
            return

        key, json_data = task_data_tuple
        task_data = _deserialize(json_data)
        entity = task_data["entity"]
        config = task_data["config"]
        dataset = task_data["dataset"]
        conn.decr(make_key('ingest', TASK_PENDING, dataset))
        conn.incr(make_key('ingest', TASK_RUNNING, dataset))
        yield (dataset, entity, config)


def get_status(dataset):
    conn = get_redis()
    pending_tasks = int(conn.get(make_key('ingest', TASK_PENDING, dataset)) or 0)  # noqa
    executing_tasks = int(conn.get(make_key('ingest', TASK_RUNNING, dataset)) or 0)  # noqa
    finished_tasks = int(conn.get(make_key('ingest', TASK_FINISHED, dataset)) or 0)  # noqa
    return {
        TASK_TOTAL: pending_tasks + executing_tasks + finished_tasks,
        TASK_FINISHED: finished_tasks,
    }


def mark_task_finished(dataset):
    conn = get_redis()
    pending = int(conn.get(make_key('ingest', TASK_PENDING, dataset)) or 0)
    executing = int(conn.decr(make_key('ingest', TASK_RUNNING, dataset)) or 0)
    conn.incr(make_key('ingest', TASK_FINISHED, dataset))
    if pending == 0 and executing == 0:
        reset_status(dataset)


def reset_status(dataset):
    conn = get_redis()
    conn.delete(make_key('ingest', TASK_PENDING, dataset))
    conn.delete(make_key('ingest', TASK_RUNNING, dataset))
    conn.delete(make_key('ingest', TASK_FINISHED, dataset))