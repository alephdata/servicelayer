from unittest import TestCase

from servicelayer.cache import get_fakeredis
from servicelayer.jobs import Job, Stage, Task, Dataset


class ProcessTest(TestCase):

    def setUp(self):
        self.conn = get_fakeredis()
        self.conn.flushall()
        self.dataset = 'test_1'

    def test_job_queue(self):
        job = Job.create(self.conn, self.dataset)
        stage = job.get_stage('ingest')
        status = stage.get_status()
        assert status['pending'] == 0
        assert status['finished'] == 0
        assert job.is_done()
        stage.queue({'test': 'foo'}, {})
        status = job.get_status()
        assert status['pending'] == 1
        assert status['finished'] == 0
        assert status['running'] == 0
        assert not job.is_done()
        task = Stage.get_task(self.conn, 'ingest',
                              timeout=None)
        assert task.job.dataset.name == job.dataset.name
        assert task.payload['test'] == 'foo'
        status = job.get_status()
        assert status['pending'] == 0
        assert status['running'] == 1
        assert status['finished'] == 0
        assert not job.is_done()
        task.done()
        status = job.get_status()
        assert status['pending'] == 0
        assert status['running'] == 0
        assert status['finished'] == 1
        assert job.is_done()

    def test_queue_clear(self):
        job = Job.create(self.conn, self.dataset)
        stage = job.get_stage('ingest')

        stage.queue({'test': 'foo'}, {})
        status = stage.get_status()
        assert status['pending'] == 1
        job.dataset.cancel()
        status = stage.get_status()
        assert status['pending'] == 0

        stage.queue({'test': 'foo'}, {})
        status = stage.get_status()
        assert status['pending'] == 1
        job.remove()
        status = stage.get_status()
        assert status['pending'] == 0

    def test_fake_finished(self):
        job = Job.create(self.conn, self.dataset)
        stage = job.get_stage('ingest')
        status = stage.get_status()
        assert status['finished'] == 0
        stage.report_finished(500)
        status = stage.get_status()
        assert status['finished'] == 500
        status = job.dataset.get_status()
        assert status['finished'] == 500, status

    def test_fetch_multiple_task(self):
        job = Job.create(self.conn, self.dataset)
        stage = job.get_stage('ingest')
        stage.queue({'test': 'foo'}, {})
        stage.queue({'test': 'bar'}, {})
        status = job.get_status()
        assert status['pending'] == 2
        tasks = list(stage.get_tasks(limit=5))
        assert len(tasks) == 2
        for task in tasks:
            assert isinstance(task, Task)
        assert tasks[0].payload == {'test': 'foo'}
        assert tasks[1].payload == {'test': 'bar'}
        job.dataset.cancel()

    def test_active_dataset_status(self):
        job = Job.create(self.conn, self.dataset)
        stage = job.get_stage('ingest')
        stage.queue({'test': 'foo'}, {})
        stage.queue({'test': 'bar'}, {})
        status = Dataset.get_active_dataset_status(self.conn)
        assert len(status['datasets']) == 1
        assert status['total'] == 1
        assert status['datasets']['test_1']['pending'] == 2
        job.dataset.cancel()
        status = Dataset.get_active_dataset_status(self.conn)
        assert status['datasets'] == {}
        assert status['total'] == 0
