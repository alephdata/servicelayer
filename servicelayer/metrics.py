from prometheus_client import (
    Counter,
    Histogram,
    REGISTRY,
    GC_COLLECTOR,
    PROCESS_COLLECTOR,
)

# These definitions should be moved as close to the place
# where they are used as possible. However, since we
# support both a homebrewed Worker and one based on
# RabbitMQ, these definitions would come into conflict.

REGISTRY.unregister(GC_COLLECTOR)
REGISTRY.unregister(PROCESS_COLLECTOR)

TASKS_STARTED = Counter(
    "servicelayer_tasks_started_total",
    "Number of tasks that a worker started processing",
    ["stage"],
)

TASKS_SUCCEEDED = Counter(
    "servicelayer_tasks_succeeded_total",
    "Number of successfully processed tasks",
    ["stage", "retries"],
)

TASKS_FAILED = Counter(
    "servicelayer_tasks_failed_total",
    "Number of failed tasks",
    ["stage", "retries", "failed_permanently"],
)

TASK_DURATION = Histogram(
    "servicelayer_task_duration_seconds",
    "Task duration in seconds",
    ["stage"],
    # The bucket sizes are a rough guess right now, we might want to adjust
    # them later based on observed runtimes
    buckets=[
        0.25,
        0.5,
        1,
        5,
        15,
        30,
        60,
        60 * 15,
        60 * 30,
        60 * 60,
        60 * 60 * 2,
        60 * 60 * 6,
        60 * 60 * 24,
    ],
)
