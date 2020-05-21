import logging

__version__ = '1.11.0'


logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
