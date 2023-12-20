import multiprocessing
from servicelayer import env

# Redis cache
# URL format: redis://localhost:6379/0
REDIS_URL = env.get("REDIS_URL")
REDIS_SHORT = 84700
REDIS_LONG = REDIS_SHORT * 200
REDIS_EXPIRE = env.to_int("REDIS_EXPIRE", REDIS_SHORT * 7)
REDIS_PREFIX = "sla"

# Persistent database tags
TAGS_DATABASE_URI = env.get("TAGS_DATABASE_URI", "sqlite://")

# Worker
WORKER_RETRY = env.to_int("WORKER_RETRY", 3)
WORKER_THREADS = env.to_int("WORKER_THREADS", multiprocessing.cpu_count())
WORKER_REPORTING = env.to_bool("WORKER_REPORTING", True)

# Amazon client credentials
AWS_KEY_ID = env.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = env.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = env.get("AWS_REGION", "eu-west-1")
# S3 compatible Minio host if using Minio for storage
ARCHIVE_ENDPOINT_URL = env.get("ARCHIVE_ENDPOINT_URL")

# Storage type (either 's3', 'gs', or 'file', i.e. local file system):
ARCHIVE_TYPE = env.get("ARCHIVE_TYPE", "file")
ARCHIVE_BUCKET = env.get("ARCHIVE_BUCKET")
ARCHIVE_PATH = env.get("ARCHIVE_PATH")
PUBLICATION_BUCKET = env.get("PUBLICATION_BUCKET", ARCHIVE_BUCKET)

# Logging
LOG_FORMAT = env.get("LOG_FORMAT", "TEXT")  # options are: TEXT or JSON

# Task queue
RABBITMQ_URL = env.get("RABBITMQ_URL", "rabbitmq")
RABBITMQ_USERNAME = env.get("RABBITMQ_USERNAME", "guest")
RABBITMQ_PASSWORD = env.get("RABBITMQ_PASSWORD", "guest")
RABBITMQ_HEARTBEAT = env.to_int("RABBITMQ_HEARTBEAT", 600)
RABBITMQ_BLOCKED_CONNECTION_TIMEOUT = env.to_int(
    "RABBITMQ_BLOCKED_CONNECTION_TIMEOUT", 300
)
RABBITMQ_MAX_PRIORITY = 10
QUEUE_ALEPH = "aleph_queue"
QUEUE_INGEST = "ingest_queue"
QUEUE_INDEX = "index_queue"

# Sentry
SENTRY_DSN = env.get("SENTRY_DSN")
SENTRY_ENVIRONMENT = env.get("SENTRY_ENVIRONMENT", "")
SENTRY_RELEASE = env.get("SENTRY_RELEASE", "")

# Instrumentation
PROMETHEUS_ENABLED = env.to_bool("PROMETHEUS_ENABLED", False)
PROMETHEUS_PORT = env.to_int("PROMETHEUS_PORT", 9100)
