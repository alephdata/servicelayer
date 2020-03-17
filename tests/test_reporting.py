from unittest import TestCase

from servicelayer.cache import get_redis
from servicelayer.jobs import Job, Stage, Task
from servicelayer.reporting import Reporter, OP_REPORT, Status
from servicelayer.util import dump_json


class ReportingTest(TestCase):
    def setUp(self):
        self.conn = get_redis()
        self.dataset = 'my-dataset'
        self.operation = 'OP_FOO'
        self.job = Job.create(self.conn, self.dataset)

    def _get_task(self):
        """return the newly created reporting task"""
        return Stage.get_task(self.conn, [OP_REPORT])

    def test_reporting(self):
        payload = {
            'msg': 'I do report.'
        }

        reporter = Reporter(stage=self.job.get_stage(self.operation))

        # start
        reporter.start(**payload)
        task = self._get_task()

        self.assertEqual(task.stage.stage, OP_REPORT)

        data = task.payload
        values = {
            'msg': 'I do report.',
            'status': Status.START,
            'has_error': False,
            'operation': self.operation,
            'dataset': self.dataset,
            'updated_at': None,
            'start_at': None,
            'job': self.job.id
        }
        for key, value in values.items():
            self.assertIn(key, data)
            if value is not None:
                self.assertEqual(value, data[key])

        # success
        reporter.end(**payload)
        task = self._get_task()
        self.assertEqual(Status.SUCCESS, task.payload['status'])
        self.assertIn('success_at', task.payload)

        # failing
        reporter.error(Exception('An error occured!'), **payload)
        task = self._get_task()
        data = task.payload
        self.assertEqual(Status.ERROR, data['status'])
        self.assertTrue(data['has_error'])
        self.assertIn('error_at', data)
        self.assertEqual('Exception', data['error_name'])
        self.assertEqual('An error occured!', data['error_msg'])

    def test_reporting_from_task(self):
        task_data = {
            'dataset': self.dataset,
            'job': self.job.id,
            'stage': self.job.get_stage(self.operation).stage,
            'payload': {
                'entities': ['a', 'b', 'c']
            }
        }
        task = Task.unpack(self.conn, dump_json(task_data))
        serialized = task.serialize()
        reporter = Reporter(task=task)
        reporter.start()

        task = self._get_task()
        self.assertEqual(task.stage.stage, OP_REPORT)

        data = task.payload
        values = {
            'status': Status.START,
            'has_error': False,
            'operation': self.operation,
            'dataset': self.dataset,
            'updated_at': None,
            'start_at': None,
            'job': self.job.id,
            'task': serialized
        }
        for key, value in values.items():
            self.assertIn(key, data)
            if value is not None:
                self.assertEqual(value, data[key])
