import sys
import signal
import logging
from threading import Thread
from banal import ensure_list
from abc import ABC, abstractmethod

from servicelayer import settings
from servicelayer.jobs import Stage
from servicelayer.cache import get_redis
from servicelayer.util import unpack_int

log = logging.getLogger(__name__)


class Worker(ABC):
    """Workers of all microservices, unite!"""

    def __init__(self, conn=None, stages=None, num_threads=settings.WORKER_THREADS):
        self.conn = conn or get_redis()
        self.stages = stages
        self.num_threads = num_threads
        self._shutdown = False

    def shutdown(self, *args):
        log.warning("Shutting down worker.")
        self._shutdown = True
        if not self.num_threads:
            sys.exit(23)

    def handle_safe(self, task):
        try:
            self.handle(task)
        except (SystemExit, KeyboardInterrupt, Exception):
            self.retry(task)
            raise
        finally:
            task.done()
            self.after_task(task)

    def init_internal(self):
        self._shutdown = False
        self.boot()

    def retry(self, task):
        retries = unpack_int(task.context.get("retries"))
        if retries < settings.WORKER_RETRY:
            log.warning("Queue failed task for re-try...")
            task.context["retries"] = retries + 1
            task.stage.queue(task.payload, task.context)

    def process(self, interval=5):
        while True:
            if self._shutdown:
                return
            self.periodic()
            stages = self.get_stages()
            task = Stage.get_task(self.conn, stages, timeout=interval)
            if task is None:
                continue
            self.handle_safe(task)

    def sync(self):
        """Process only the tasks already in the job queue, but do not
        go into an infinte loop waiting for new ones."""
        self.init_internal()
        while True:
            stages = self.get_stages()
            task = Stage.get_task(self.conn, stages, timeout=None)
            if task is None:
                return
            self.handle_safe(task)

    def run(self):
        # Try to quit gracefully, e.g. after finishing the current task.
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        self.init_internal()
        if not self.num_threads:
            return self.process()
        log.info("Worker has %d threads.", self.num_threads)
        threads = []
        for _ in range(self.num_threads):
            thread = Thread(target=self.process)
            thread.daemon = True
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()
        sys.exit(23)

    def get_stages(self):
        """Easily allow the user to make the active stages dynamic."""
        return self.stages

    def boot(self):
        """Optional hook for the boot-up of the worker."""
        pass

    def periodic(self):
        """Optional hook for running a periodic task checker."""
        pass

    def after_task(self, task):
        """Optional hook excuted after handling a task"""
        pass

    def dispatch_pipeline(self, task, payload):
        """Some queues use a continuation passing style pipeline argument
        to specify the next steps for a processing chain."""
        pipeline = ensure_list(task.context.get("pipeline"))
        if len(pipeline) == 0:
            return
        next_stage = pipeline.pop(0)
        stage = task.job.get_stage(next_stage)
        context = dict(task.context)
        context["pipeline"] = pipeline
        stage.queue(payload, context)

    @abstractmethod
    def handle(self, task):
        raise NotImplementedError
