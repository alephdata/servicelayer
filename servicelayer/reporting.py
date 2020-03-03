from datetime import datetime

from normality import stringify

from servicelayer.jobs import Task
from servicelayer.settings import WORKER_REPORTING
from servicelayer.util import dump_json


OP_REPORT = 'report'


class Status:
    START = 'start'
    END = 'end'
    ERROR = 'error'


class TaskReporter:
    def __init__(
            self,
            conn,
            task=None,
            job=None,
            operation=None,
            dataset=None,
            **defaults):
        self.conn = conn
        self.task = task
        self.job = job
        self.operation = operation
        self.dataset = dataset
        self.defaults = defaults

    def start(self, **data):
        self.handle(status=Status.START, **data)

    def end(self, **data):
        self.handle(status=Status.END, **data)

    def error(self, exception, **data):
        self.handle(status=Status.ERROR, exception=exception, **data)

    def get_report_data(self, job, status, operation, dataset, **extra):
        now = datetime.now()
        exception = extra.pop('exception', None)
        data = {**self.defaults, **{
            'job': job,
            'updated_at': now,
            '%s_at' % status: now,
            'operation': operation,
            'status': status,
            'dataset': dataset
        }, **extra}
        if exception:
            data.update({
                'status': 'error',
                'has_error': True,
                'error_name': exception.__class__.__name__,
                'error_msg': stringify(exception)
            })
        if self.task:
            data.update(task=self.task.serialize())
        return data

    def handle(self, **data):
        if WORKER_REPORTING:
            if self.task:
                self._handle_task(self.task, **data)
            else:
                self._handle_data(self.operation, self.dataset, self.job, **data)

    def _handle_task(self, task, **extra):
        """queue a new reporting task based on given `task`"""
        status = extra.pop('status')
        operation = extra.pop('operation', None)
        payload = self.get_report_data(
            job=task.job.id,
            status=status,
            operation=operation or task.stage.stage,
            dataset=task.job.dataset.name,
            dump=task.serialize(),
            **extra
        )
        stage = task.job.get_stage(OP_REPORT)
        stage.queue(payload)

    def _handle_data(self, operation, dataset, job, **data):
        """queue a new reporting task based on `data`"""
        task = Task.unpack(self.conn, dump_json({
            'operation': operation,
            'dataset': dataset,
            'job': job
        }))
        self._handle_task(task, **data)

    def from_task(self, task):
        return TaskReporter(conn=self.conn, task=task)

    def copy(self, **data):
        """return a new reporter with updated data"""
        base = {
            'task': self.task,
            'job': self.job,
            'operation': self.operation,
            'dataset': self.dataset
        }
        return TaskReporter(self.conn, **{**base, **self.defaults, **data})
