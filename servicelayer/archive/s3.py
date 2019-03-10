import os
import boto3
import logging
from botocore.exceptions import ClientError

from servicelayer import settings
from servicelayer.archive.virtual import VirtualArchive
from servicelayer.archive.util import checksum

log = logging.getLogger(__name__)


class S3Archive(VirtualArchive):
    TIMEOUT = 84600

    def __init__(self, bucket=None):
        super(S3Archive, self).__init__(bucket)
        key_id = settings.AWS_KEY_ID
        secret_key = settings.AWS_SECRET_KEY
        self.client = boto3.client('s3',
                                   region_name=settings.AWS_REGION,
                                   aws_access_key_id=key_id,
                                   aws_secret_access_key=secret_key)
        # config=Config(signature_version='s3v4'))
        self.bucket = bucket
        log.info("Archive: s3://%s", bucket)

        try:
            self.client.head_bucket(Bucket=bucket)
        except ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                self.client.create_bucket(Bucket=bucket)
            else:
                log.exception("Could not check bucket")

        # Make sure bucket policy is set correctly.
        config = {
            'CORSRules': [
                {
                    'AllowedMethods': ['GET'],
                    'AllowedOrigins': ['*'],
                    'AllowedHeaders': ['*'],
                    'ExposeHeaders': ['Accept-Ranges', 'Content-Encoding',
                                      'Content-Length', 'Content-Range'],
                    'MaxAgeSeconds': self.TIMEOUT
                }
            ]
        }
        try:
            self.client.put_bucket_cors(Bucket=bucket,
                                        CORSConfiguration=config)
        except ClientError:
            log.exception("Could not update CORS")

    def _locate_key(self, content_hash):
        """Check if a file with the given hash exists on S3."""
        if content_hash is None:
            return
        prefix = self._get_prefix(content_hash)
        if prefix is None:
            return
        # obj = self.client.Object(self.bucket, os.path.join(prefix, 'data'))
        res = self.client.list_objects(MaxKeys=1,
                                       Bucket=self.bucket,
                                       Prefix=prefix)
        for obj in res.get('Contents', []):
            return obj.get('Key')

    def archive_file(self, file_path, content_hash=None):
        """Store the file located at the given path on S3, based on a path
        made up from its SHA1 content hash."""
        if content_hash is None:
            content_hash = checksum(file_path)

        obj = self._locate_key(content_hash)
        if obj is None:
            path = os.path.join(self._get_prefix(content_hash), 'data')
            self.client.upload_file(file_path, self.bucket, path)
        return content_hash

    def load_file(self, content_hash, file_name=None, temp_path=None):
        """Retrieve a file from S3 storage and put it onto the local file
        system for further processing."""
        key = self._locate_key(content_hash)
        if key is not None:
            path = self._local_path(content_hash, file_name, temp_path)
            self.client.download_file(self.bucket, key, path)
            return path

    def generate_url(self, content_hash, file_name=None, mime_type=None):
        key = self._locate_key(content_hash)
        if key is None:
            return
        params = {
            'Bucket': self.bucket,
            'Key': key
        }
        if mime_type is not None:
            params['ResponseContentType'] = mime_type
        if file_name is not None:
            disposition = 'inline; filename=%s' % file_name
            params['ResponseContentDisposition'] = disposition
        return self.client.generate_presigned_url('get_object',
                                                  Params=params,
                                                  ExpiresIn=self.TIMEOUT)
