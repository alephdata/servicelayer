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
WORKER_THREADS = env.to_int('WORKER_THREADS', multiprocessing.cpu_count())
WORKER_REPORTING = env.to_bool('WORKER_REPORTING', True)

# Amazon client credentials
AWS_KEY_ID = env.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = env.get('AWS_SECRET_ACCESS_KEY')
AWS_REGION = env.get('AWS_REGION', 'eu-west-1')

# Storage type (either 's3', 'gs', or 'file', i.e. local file system):
ARCHIVE_TYPE = env.get('ARCHIVE_TYPE', 'file')
ARCHIVE_BUCKET = env.get('ARCHIVE_BUCKET')
ARCHIVE_PATH = env.get('ARCHIVE_PATH')
PUBLICATION_BUCKET = env.get('PUBLICATION_BUCKET')
PUBLICATION_PATH = env.get('PUBLICATION_PATH')

# Logging
LOGGING_LEVEL = env.get('LOGGING_LEVEL', 'DEBUG').upper()
LOGGING_FORMAT = '%(levelname)s:%(name)s:%(message)s'
LOGGING_FORMAT = env.get('LOGGING_FORMAT', LOGGING_FORMAT)
