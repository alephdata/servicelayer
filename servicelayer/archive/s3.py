import boto3
import logging
from datetime import datetime
from botocore.exceptions import ClientError

from servicelayer import settings
from servicelayer.archive.virtual import VirtualArchive
from servicelayer.archive.util import checksum, ensure_path
from servicelayer.archive.util import path_prefix, path_content_hash

log = logging.getLogger(__name__)


class S3Archive(VirtualArchive):
    TIMEOUT = 84600

    def __init__(self, bucket=None, publication_bucket=None):
        super(S3Archive, self).__init__(bucket)
        key_id = settings.AWS_KEY_ID
        secret_key = settings.AWS_SECRET_KEY
        self.client = boto3.client(
            "s3",
            endpoint_url=settings.ARCHIVE_ENDPOINT_URL,
            region_name=settings.AWS_REGION,
            aws_access_key_id=key_id,
            aws_secret_access_key=secret_key,
        )
        # config=Config(signature_version='s3v4'))
        self.bucket = bucket
        self.publication_bucket = publication_bucket
        log.info("Archive: s3://%s", bucket)

        try:
            self.client.head_bucket(Bucket=bucket)
        except ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                self.client.create_bucket(
                    Bucket=bucket,
                    CreateBucketConfiguration={
                        "LocationConstraint": settings.AWS_REGION
                    },
                )
                self.upgrade()
            else:
                log.exception("Could not check bucket")

    def upgrade(self):
        # Make sure bucket policy is set correctly.
        config = {
            "CORSRules": [
                {
                    "AllowedMethods": ["GET"],
                    "AllowedOrigins": ["*"],
                    "AllowedHeaders": ["*"],
                    "ExposeHeaders": [
                        "Accept-Ranges",
                        "Content-Encoding",
                        "Content-Length",
                        "Content-Range",
                    ],
                    "MaxAgeSeconds": self.TIMEOUT,
                }
            ]
        }
        try:
            self.client.put_bucket_cors(Bucket=self.bucket, CORSConfiguration=config)
        except ClientError:
            log.exception("Could not update CORS")

    def _locate_key(self, content_hash=None, prefix=None):
        """Check if a file with the given hash exists on S3."""
        if prefix is None:
            if content_hash is None:
                return
            prefix = path_prefix(content_hash)
            if prefix is None:
                return
        res = self.client.list_objects(MaxKeys=1, Bucket=self.bucket, Prefix=prefix)
        for obj in res.get("Contents", []):
            return obj.get("Key")

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

        path = "{}/data".format(path_prefix(content_hash))
        extra = {}
        if mime_type is not None:
            extra["ContentType"] = mime_type
        with open(file_path, "rb") as fh:
            self.client.upload_fileobj(fh, self.bucket, str(path), ExtraArgs=extra)
        return content_hash

    def load_file(self, content_hash, file_name=None, temp_path=None):
        """Retrieve a file from S3 storage and put it onto the local file
        system for further processing."""
        key = self._locate_key(content_hash)
        if key is not None:
            path = self._local_path(content_hash, file_name, temp_path)
            self.client.download_file(self.bucket, key, str(path))
            return path

    def delete_file(self, content_hash):
        if content_hash is None:
            return
        prefix = path_prefix(content_hash)
        if prefix is None:
            return
        res = self.client.list_objects(Bucket=self.bucket, Prefix=prefix)
        for obj in res.get("Contents", []):
            self.client.delete_object(Bucket=self.bucket, Key=obj.get("Key"))

    def list_files(self, prefix=None):
        """Try to list out all the hashes in the archive."""
        kwargs = {"Bucket": self.bucket}
        prefix = path_prefix(prefix)
        if prefix is not None:
            kwargs["Prefix"] = prefix
        token = None
        while True:
            if token is not None:
                kwargs["ContinuationToken"] = token
            res = self.client.list_objects_v2(**kwargs)
            for obj in res.get("Contents", []):
                yield path_content_hash(obj.get("Key"))
            if not res.get("IsTruncated"):
                break
            token = res.get("NextContinuationToken")

    def generate_url(self, content_hash, file_name=None, mime_type=None, expire=None):
        key = self._locate_key(content_hash)
        if key is None:
            return
        params = {"Bucket": self.bucket, "Key": key}
        if mime_type is not None:
            params["ResponseContentType"] = mime_type
        if file_name is not None:
            disposition = "attachment; filename=%s" % file_name
            params["ResponseContentDisposition"] = disposition
        expires_in = self.TIMEOUT
        if expire is not None:
            delta = expire - datetime.utcnow()
            expires_in = int(delta.total_seconds())
        return self.client.generate_presigned_url(
            "get_object", Params=params, ExpiresIn=expires_in
        )

    @property
    def can_publish(self):
        return True

    def publish_file(self, file_path, publish_path, mime_type=None):
        extra = {"ACL": "public-read"}
        if mime_type is not None:
            extra["ContentType"] = mime_type
        with open(file_path, "rb") as fh:
            self.client.upload_fileobj(
                fh, self.publication_bucket, publish_path, ExtraArgs=extra
            )
        params = {"Bucket": self.publication_bucket, "Key": publish_path}
        if mime_type is not None:
            params["ResponseContentType"] = mime_type
        return self.client.generate_presigned_url(
            "get_object", Params=params, ExpiresIn=0
        )
