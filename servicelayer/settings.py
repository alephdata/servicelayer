import multiprocessing
from servicelayer import env

# Redis cache
REDIS_URL = env.get('REDIS_URL')
REDIS_SHORT = 84700
REDIS_LONG = REDIS_SHORT * 200
REDIS_EXPIRE = env.to_int('REDIS_EXPIRE', REDIS_SHORT * 7)
REDIS_PREFIX = 'sla'

# Worker
WORKER_RETRY = env.to_int('WORKER_RETRY', 3)
WORKER_THREADS = min(8, multiprocessing.cpu_count())
WORKER_THREADS = env.to_int('WORKER_THREADS', WORKER_THREADS)

# Aleph client API settings
ALEPH_HOST = env.get('MEMORIOUS_ALEPH_HOST')
ALEPH_HOST = env.get('ALEPH_HOST', ALEPH_HOST)

ALEPH_API_KEY = env.get('MEMORIOUS_ALEPH_API_KEY')
ALEPH_API_KEY = env.get('ALEPH_API_KEY', ALEPH_API_KEY)

# Amazon client credentials
AWS_KEY_ID = env.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = env.get('AWS_SECRET_ACCESS_KEY')
AWS_REGION = env.get('AWS_REGION', 'eu-west-1')

# Storage type (either 's3', 'gs', or 'file', i.e. local file system):
ARCHIVE_TYPE = env.get('ARCHIVE_TYPE', 'file')
ARCHIVE_BUCKET = env.get('ARCHIVE_BUCKET')
ARCHIVE_PATH = env.get('ARCHIVE_PATH')
