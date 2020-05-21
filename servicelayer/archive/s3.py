import boto3
import logging
from botocore.exceptions import ClientError
from normality import safe_filename

from servicelayer import settings
from servicelayer.archive.virtual import VirtualArchive
from servicelayer.archive.util import checksum, ensure_path

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
                self.upgrade()
            else:
                log.exception("Could not check bucket")

    def upgrade(self):
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
            self.client.put_bucket_cors(Bucket=self.bucket,
                                        CORSConfiguration=config)
        except ClientError:
            log.exception("Could not update CORS")

    def _locate_key(self, content_hash=None, prefix=None):
        """Check if a file with the given hash exists on S3."""
        if prefix is None:
            if content_hash is None:
                return
            prefix = self._get_prefix(content_hash)
            if prefix is None:
                return
        res = self.client.list_objects(MaxKeys=1,
                                       Bucket=self.bucket,
                                       Prefix=prefix)
        for obj in res.get('Contents', []):
            return obj.get('Key')

    def archive_file(self, file_path, content_hash=None, mime_type=None):
        """Store the file located at the given path on S3, based on a path
        made up from its SHA1 content hash."""
        file_path = ensure_path(file_path)
        if content_hash is None:
            content_hash = checksum(file_path)

        # if content_hash is None:
        #     return

        obj = self._locate_key(content_hash)
        if obj is not None:
            return content_hash

        path = '{}/data'.format(self._get_prefix(content_hash))
        extra = {}
        if mime_type is not None:
            extra['ContentType'] = mime_type
        with open(file_path, 'rb') as fh:
            self.client.upload_fileobj(fh, self.bucket, str(path),
                                       ExtraArgs=extra)
        return content_hash

    def load_file(self, content_hash, file_name=None, temp_path=None):
        """Retrieve a file from S3 storage and put it onto the local file
        system for further processing."""
        key = self._locate_key(content_hash)
        if key is not None:
            path = self._local_path(content_hash, file_name, temp_path)
            self.client.download_file(self.bucket, key, str(path))
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

    def publish(self, namespace, file_path, mime_type=None):
        file_path = ensure_path(file_path)
        file_name = safe_filename(file_path, default='data')
        store_path = '{0}/{1}'.format(namespace, file_name)
        extra = {}
        if mime_type is not None:
            extra['ContentType'] = mime_type
        with open(file_path, 'rb') as fh:
            self.client.upload_fileobj(fh, self.bucket, str(store_path),
                                       ExtraArgs=extra)

    def delete_publication(self, namespace, file_name):
        key = '{0}/{1}'.format(namespace, file_name)
        key = self._locate_key(prefix=key)
        if key is not None:
            self.client.delete_object(Bucket=self.bucket, Key=key)

    def generate_publication_url(self, namespace, file_name, mime_type=None,
                                 expire=None):
        key = '{0}/{1}'.format(namespace, file_name)
        key = self._locate_key(prefix=key)
        if key is None:
            return
        params = {
            'Bucket': self.bucket,
            'Key': key
        }
        if mime_type is not None:
            params['ResponseContentType'] = mime_type
        disposition = 'attachment; filename=%s' % file_name
        params['ResponseContentDisposition'] = disposition
        expire = expire or self.TIMEOUT
        return self.client.generate_presigned_url('get_object',
                                                  Params=params,
                                                  ExpiresIn=expire)
