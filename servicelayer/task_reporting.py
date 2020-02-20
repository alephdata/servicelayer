from datetime import datetime

from normality import stringify

from servicelayer.jobs import Task
from servicelayer.util import dump_json


class Base:
    START = 'start'
    END = 'end'
    ERROR = 'error'
    OP_REPORT = 'report'


class Handler(Base):
    def __init__(self, handle, *args, **kwargs):
        self.handle = handle
        self.args = args
        self.kwargs = kwargs

    def start(self, **kwargs):
        return self.handle(status=self.START, *self.args, **{**self.kwargs, **kwargs})

    def end(self, **kwargs):
        return self.handle(status=self.END, *self.args, **{**self.kwargs, **kwargs})

    def error(self, **kwargs):
        return self.handle(status=self.ERROR, *self.args, **{**self.kwargs, **kwargs})


class TaskReporter(Base):
    def __init__(
            self,
            conn,
            task=None,
            job=None,
            stage=None,
            dataset=None,
            status=None,
            clean_payload=None,
            **defaults):
        self.conn = conn
        self.task = task
        self.job = job
        self.stage = stage
        self.dataset = dataset
        self.status = status
        self.clean_payload = clean_payload
        self.defaults = defaults
        self.handler = self.get_handler()

    def start(self, **kwargs):
        self.handler.start(**kwargs)

    def end(self, **kwargs):
        self.handler.end(**kwargs)

    def error(self, exception, **kwargs):
        self.handler.error(exception=exception, **kwargs)

    def set(self, **data):
        for k, v in data.items():
            if hasattr(self, k):
                setattr(self, k, v)
            else:
                self.defaults[k] = v

    def get_report_data(self, job, status, stage, dataset, dump=None, **extra):
        now = datetime.now()
        exception = extra.pop('exception', None)
        data = {**self.defaults, **{
            'job': job,
            'updated_at': now,
            '%s_at' % status: now,
            'stage': stage,
            'status': status,
            'dataset': dataset,
            'original_dump': dump
        }, **extra}
        if exception:
            data.update({
                'status': 'error',
                'has_error': True,
                'error_name': exception.__class__.__name__,
                'error_msg': stringify(exception)
            })
        return data

    def task_handler(self, task, **extra):
        """queue a new reporting task based on given `task`"""
        status = extra.pop('status')
        stage = extra.pop('stage', None)
        payload = self.get_report_data(
            job=task.job.id,
            status=status,
            stage=stage or task.stage.stage,
            dataset=task.job.dataset.name,
            dump=task.serialize(),
            **extra
        )
        if self.clean_payload:
            payload = self.clean_payload(payload)
        stage = task.job.get_stage(self.OP_REPORT)
        stage.queue(payload)

    def data_handler(self, stage, dataset, job, **data):
        """queue a new reporting task based on `data`"""
        task = self._create_proxy_task(stage, dataset, job)
        self.task_handler(task, **data)

    def get_handler(self):
        if self.task:
            return Handler(self.task_handler, task=self.task)
        return Handler(self.data_handler, self.stage, self.dataset, self.job)

    def _create_proxy_task(self, stage, dataset, job):
        return Task.unpack(self.conn, dump_json({
            'stage': stage,
            'dataset': dataset,
            'job': job
        }))

    def serialize(self, **defaults):
        data = {**{
            'job': self.job,
            'stage': self.stage,
            'dataset': self.dataset
        }, **{**self.defaults, **defaults}}
        # FIXME
        data.pop('ns', None)
        return data

    def copy(self, **data):
        """return a new reporter with updated data"""
        base = {
            'task': self.task,
            'job': self.job,
            'stage': self.stage,
            'dataset': self.dataset,
            'status': self.status,
            'clean_payload': self.clean_payload
        }
        return TaskReporter(self.conn, **{**base, **self.defaults, **data})

    def from_data(self, data, **extra):
        """load a reporter from serialized data"""
        return TaskReporter(self.conn, **{**self.defaults, **data, **extra})
