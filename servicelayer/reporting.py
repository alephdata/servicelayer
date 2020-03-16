from datetime import datetime
from normality import stringify

from servicelayer.jobs import Job
from servicelayer.settings import WORKER_REPORTING


OP_REPORT = 'report'


class Status:
    START = 'start'
    SUCCESS = 'success'
    ERROR = 'error'


class Reporter(object):
    """A reporter will create processing status updates."""

    def __init__(self, task=None, stage=None, operation=None):
        self.task = task
        self.stage = stage
        self.operation = operation

    def start(self, **data):
        self.handle(status=Status.START, **data)

    def end(self, **data):
        self.handle(status=Status.SUCCESS, **data)

    def error(self, exception, **data):
        self.handle(status=Status.ERROR, exception=exception, **data)

    def handle(self, status, operation=None, exception=None, task=None, **payload):
        """Report a processing event that may be related to a task."""
        if not WORKER_REPORTING:
            return

        task = task or self.task
        if task is not None:
            payload['task'] = task.serialize()
            stage = task.stage
        else:
            stage = self.stage
        dataset = stage.job.dataset.name
        job_id = stage.job.id
        operation = operation or stage.stage

        now = datetime.utcnow()
        payload.update({
            'dataset': dataset,
            'operation': operation,
            'job': job_id,
            'status': status,
            'updated_at': now,
            '%s_at' % status: now,
            'has_error': False,
        })

        if exception is not None:
            payload.update({
                'status': Status.ERROR,
                'has_error': True,
                'error_name': exception.__class__.__name__,
                'error_msg': stringify(exception)
            })

        job = Job(stage.conn, dataset, job_id)
        stage = job.get_stage(OP_REPORT)
        stage.queue(payload)
