from unittest import TestCase

from servicelayer.cache import get_fakeredis
from servicelayer.process import Job, RateLimit, Progress


class ProcessTest(TestCase):

    def test_service_queue(self):
        conn = get_fakeredis()
        ds = 'test_1'
        job_id = 'job_1'
        job = Job(conn, Job.OP_INGEST, job_id, ds)
        status = job.progress.get()
        assert status['pending'] == 0
        assert status['finished'] == 0
        assert job.is_done()
        job.queue_task({'test': 'foo'}, {})
        status = job.progress.get()
        assert status['pending'] == 1
        assert status['finished'] == 0
        assert not job.is_done()
        task = Job.get_operation_task(conn, Job.OP_INGEST, timeout=None)
        nq, payload, context = task
        assert nq.dataset == job.dataset
        assert payload['test'] == 'foo'
        status = job.progress.get()
        assert status['pending'] == 1
        assert status['finished'] == 0
        job.task_done()
        status = job.progress.get()
        assert status['pending'] == 0
        assert status['finished'] == 1
        task = Job.get_operation_task(conn, Job.OP_INGEST, timeout=1)
        nq, payload, context = task
        assert payload is None
        job.remove()
        status = job.progress.get()
        assert status['pending'] == 0
        assert status['finished'] == 0

    def test_queue_clear(self):
        conn = get_fakeredis()
        ds = 'test_1'
        job_id = 'job_1'
        job = Job(conn, Job.OP_INGEST, job_id, ds)
        job.queue_task({'test': 'foo'}, {})
        status = job.progress.get()
        assert status['pending'] == 1
        Job.remove_dataset(conn, ds)
        status = job.progress.get()
        assert status['pending'] == 0

    def test_progress(self):
        conn = get_fakeredis()
        ds = 'test_2'
        job_id = 'job_2'
        progress = Progress(conn, Job.OP_INGEST, job_id, ds)
        status = progress.get()
        assert status['pending'] == 0
        assert status['finished'] == 0
        progress.mark_pending()
        status = progress.get()
        assert status['pending'] == 1
        assert status['finished'] == 0
        full = Progress.get_dataset_status(conn, ds)
        assert full['pending'] == 1
        job_status = Progress.get_job_status(conn, ds, job_id)
        assert job_status['pending'] == 1
        progress.mark_finished()
        status = progress.get()
        assert status['pending'] == 0
        assert status['finished'] == 1
        job_status = Progress.get_job_status(conn, ds, job_id)
        assert job_status['pending'] == 0
        assert job_status['finished'] == 1
        progress.remove()
        status = progress.get()
        assert status['finished'] == 0

    def test_rate(self):
        conn = get_fakeredis()
        limit = RateLimit(conn, 'banana', limit=10)
        assert limit.check()
        limit.update()
        assert limit.check()
        for i in range(13):
            limit.update()
        assert not limit.check()
