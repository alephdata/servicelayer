import json
import logging
from random import shuffle
import time

from servicelayer import settings
from servicelayer.cache import get_redis, make_key

TASK_PENDING = 'pending_tasks'
TASK_RUNNING = 'executing_tasks'
TASK_FINISHED = 'finished_tasks'
TASK_TOTAL = 'total_tasks'
INGESTION_FINISHED = 'ingestion_done'
SLEEP = 5


log = logging.getLogger(__name__)


def _serialize(data):
    return json.dumps(data)


def _deserialize(data):
    return json.loads(data)


def push_task(priority, dataset, entity, context):
    assert priority in settings.QUEUE_PRIORITIES
    conn = get_redis()
    conn.sadd(make_key('ingest', 'queues', priority), dataset)
    # remove dataset from queue-sets of other priorities
    for pr in settings.QUEUE_PRIORITIES:
        if pr != priority:
            conn.srem(make_key('ingest', 'queues', pr), dataset)
    conn.rpush(make_key('ingest', 'queue', dataset), _serialize({
        'dataset': dataset,
        'entity': entity,
        'context': context,
    }))
    conn.incr(make_key('ingest', TASK_PENDING, dataset))


def _get_queues():
    conn = get_redis()
    queues = []
    # Sort the queues by priority and shuffle the queues with the same
    # priority
    for priority in settings.QUEUE_PRIORITIES:
        datasets = list(conn.smembers(make_key('ingest', 'queues', priority)))
        shuffle(datasets)
        queues = queues + datasets
    queues = [make_key('ingest', 'queue', ds) for ds in queues]
    return queues


def poll_task():
    conn = get_redis()
    while True:
        # Fetch queues afresh because they may have been updated
        queues = _get_queues()
        log.info(queues)
        if not queues:
            time.sleep(SLEEP)
            continue
        task_data_tuple = conn.blpop(queues)
        # blpop blocks until it finds something. But fakeredis has no
        # blocking support. So it justs returns None.
        if task_data_tuple is None:
            return

        key, json_data = task_data_tuple
        task_data = _deserialize(json_data)
        entity = task_data["entity"]
        context = task_data["context"]
        dataset = task_data["dataset"]
        conn.decr(make_key('ingest', TASK_PENDING, dataset))
        conn.incr(make_key('ingest', TASK_RUNNING, dataset))
        yield (dataset, entity, context)


def get_status(dataset):
    conn = get_redis()
    pending_tasks = int(conn.get(make_key('ingest', TASK_PENDING, dataset)) or 0)  # noqa
    executing_tasks = int(conn.get(make_key('ingest', TASK_RUNNING, dataset)) or 0)  # noqa
    finished_tasks = int(conn.get(make_key('ingest', TASK_FINISHED, dataset)) or 0)  # noqa
    total_tasks = pending_tasks + executing_tasks + finished_tasks
    # For a dataset that has been fully ingested, TASK_TOTAL and TASK_FINISHED
    # will both be 0
    ingestion_finished = False
    if (total_tasks == 0 and finished_tasks == 0 and
            conn.sismember(make_key('ingest', 'queues', 'finished'), dataset)):
        ingestion_finished = True
    return {
        TASK_TOTAL: total_tasks,
        TASK_FINISHED: finished_tasks,
        INGESTION_FINISHED: ingestion_finished,
    }


def mark_task_finished(dataset):
    conn = get_redis()
    pending = int(conn.get(make_key('ingest', TASK_PENDING, dataset)) or 0)
    executing = int(conn.decr(make_key('ingest', TASK_RUNNING, dataset)) or 0)
    conn.incr(make_key('ingest', TASK_FINISHED, dataset))
    if pending == 0 and executing == 0:
        # All tasks are done; add it to the set of finished datasets so that
        # we can push the ingested entities to Aleph
        conn.sadd(make_key('ingest', 'queues', 'finished'), dataset)
        reset_status(dataset)


def reset_status(dataset):
    conn = get_redis()
    conn.delete(make_key('ingest', TASK_PENDING, dataset))
    conn.delete(make_key('ingest', TASK_RUNNING, dataset))
    conn.delete(make_key('ingest', TASK_FINISHED, dataset))
    conn.delete(make_key('ingest', 'queue', dataset))
