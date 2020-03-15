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

    def __init__(self, conn, task=None, stage=None,
                 operation=None, dataset=None):
        self.conn = conn
        self.task = task
        # TODO: if we just require a `stage` to be passed in, we always have
        # `conn`, `job_id` and `dataset`. Is this possible?
        self.stage = stage
        self.operation = operation
        self.dataset = dataset

    def start(self, **data):
        self.handle(status=Status.START, **data)

    def end(self, **data):
        self.handle(status=Status.END, **data)

    def error(self, exception, **data):
        self.handle(status=Status.ERROR, exception=exception, **data)

    def handle(self, status, job_id=None, operation=None, dataset=None,
               exception=None, task=None, stage=None, **payload):
        """Report a processing event that may be related to a task."""
        if not WORKER_REPORTING:
            return

        task = task or self.task
        if task is not None:
            stage = stage or task.stage
            payload['task'] = task.serialize()

        dataset = dataset or self.dataset
        stage = stage or self.stage
        if stage is not None:
            operation = operation or stage.stage
            job_id = job_id or stage.job.id
            dataset = dataset or stage.job.dataset.name

        now = datetime.utcnow()
        payload.update({
            'dataset': dataset,
            'operation': operation,
            'job_id': job_id,
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

        job = Job(self.conn, dataset, job_id)
        stage = job.get_stage(OP_REPORT)
        stage.queue(payload)
