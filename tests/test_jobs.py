from unittest import TestCase

from servicelayer.cache import get_fakeredis
from servicelayer.jobs import Job, JobStage, RateLimit, Progress, Task


class ProcessTest(TestCase):

    def test_job_queue(self):
        conn = get_fakeredis()
        conn.flushall()
        ds = 'test_1'
        job_id = 'job_1'
        job_stage = JobStage(conn, JobStage.INGEST, job_id, ds)
        status = job_stage.progress.get()
        assert status['pending'] == 0
        assert status['finished'] == 0
        assert job_stage.is_done()
        task1 = Task(job_stage, {'test': 'foo'}, {})
        job_stage.queue_task(task1)
        status = job_stage.progress.get()
        assert status['pending'] == 1
        assert status['finished'] == 0
        assert not job_stage.is_done()
        task = JobStage.get_stage_task(conn, JobStage.INGEST, timeout=None)
        assert task.stage.dataset == job_stage.dataset
        assert task.payload['test'] == 'foo'
        status = job_stage.progress.get()
        assert status['pending'] == 1
        assert status['finished'] == 0
        task2 = Task(job_stage, {'test': 'bar'}, {})
        job_stage.queue_task(task2)
        status = job_stage.progress.get()
        assert status['pending'] == 2
        assert status['finished'] == 0
        task1.done()
        status = job_stage.progress.get()
        assert status['pending'] == 1
        assert status['finished'] == 1
        # Test that pending count remains 1 due to syncing
        task2.done()
        status = job_stage.progress.get()
        assert status['pending'] == 1
        assert status['finished'] == 2
        task = JobStage.get_stage_task(conn, JobStage.INGEST, timeout=1)
        assert task is not None
        assert isinstance(task, Task)
        task = JobStage.get_stage_task(conn, JobStage.INGEST, timeout=1)
        assert task is None

    def test_queue_clear(self):
        conn = get_fakeredis()
        conn.flushall()
        ds = 'test_1'
        job_id = 'job_1'
        job_stage = JobStage(conn, JobStage.INGEST, job_id, ds)
        task = Task(job_stage, {'test': 'foo'}, {})
        job_stage.queue_task(task)
        status = job_stage.progress.get()
        assert status['pending'] == 1
        Job.remove_dataset(conn, ds)
        status = job_stage.progress.get()
        assert status['pending'] == 0
        assert Progress.get_dataset_job_ids(conn, ds) == []

    def test_execute_if_job_done(self):
        conn = get_fakeredis()
        conn.flushall()
        ds = 'test_1'
        job_id = 'job_1'
        job_stage = JobStage(conn, JobStage.INGEST, job_id, ds)
        task1 = Task(job_stage, {'test': 'foo'}, {})
        task2 = Task(job_stage, {'test': 'bar'}, {})
        task1.queue()
        task2.queue()
        status = job_stage.progress.get()
        assert status['pending'] == 2
        test_list = []

        def callback(test_list):
            test_list.append(1)

        task = JobStage.get_stage_task(conn, JobStage.INGEST, timeout=1)
        job_stage.task_done(task)
        status = job_stage.progress.get()
        assert status['pending'] == 1
        job_stage.job.execute_if_done(callback, test_list)
        # callback shouldn't be executed
        print(test_list)
        assert len(test_list) == 0
        task = JobStage.get_stage_task(conn, JobStage.INGEST, timeout=1)
        job_stage.task_done(task)
        status = job_stage.progress.get()
        assert status['pending'] == 0
        job_stage.job.execute_if_done(callback, test_list)
        # callback should have executed
        print(test_list)
        assert len(test_list) == 1

    def test_fetch_multiple_task(self):
        conn = get_fakeredis()
        conn.flushall()
        ds = 'test_1'
        job_id = 'job_1'
        job_stage = JobStage(conn, JobStage.INGEST, job_id, ds)
        task1 = Task(job_stage, {'test': 'foo'}, {})
        task2 = Task(job_stage, {'test': 'bar'}, {})
        job_stage.queue_task(task1)
        job_stage.queue_task(task2)
        status = job_stage.progress.get()
        assert status['pending'] == 2
        tasks = list(job_stage.get_tasks(limit=5))
        assert len(tasks) == 2
        for task in tasks:
            assert isinstance(task, Task)
        assert tasks[0].payload == {'test': 'foo'}
        assert tasks[1].payload == {'test': 'bar'}
        Job.remove_dataset(conn, ds)

    def test_progress(self):
        conn = get_fakeredis()
        ds = 'test_2'
        job_id = 'job_2'
        progress = Progress(conn, JobStage.INGEST, job_id, ds)
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
