import os
import logging
import threading
from datetime import datetime, timedelta
from google.cloud.storage import Blob
from google.cloud.storage.client import Client
from google.api_core.exceptions import TooManyRequests, InternalServerError
from google.api_core.exceptions import ServiceUnavailable, NotFound
from google.resumable_media.common import DataCorruption, InvalidResponse

from servicelayer.archive.virtual import VirtualArchive
from servicelayer.archive.util import checksum, ensure_path
from servicelayer.archive.util import path_prefix, ensure_posix_path
from servicelayer.archive.util import path_content_hash, HASH_LENGTH
from servicelayer.util import service_retries, backoff

log = logging.getLogger(__name__)
FAILURES = (
    TooManyRequests,
    InternalServerError,
    ServiceUnavailable,
    DataCorruption,
    InvalidResponse,
)


class GoogleStorageArchive(VirtualArchive):
    TIMEOUT = 84600

    def __init__(self, bucket=None, publication_bucket=None):
        super(GoogleStorageArchive, self).__init__(bucket)
        log.info("Archive: gs://%s", bucket)
        self._bucket = bucket
        self.local = threading.local()
        self._publication_bucket = publication_bucket

    @property
    def bucket(self):
        if not hasattr(self.local, "bucket"):
            self.local.bucket = self.client.lookup_bucket(self._bucket)
            if self.bucket is None:
                self.local.bucket = self.client.create_bucket(self._bucket)
                self.upgrade()
        return self.local.bucket

    @property
    def client(self):
        if not hasattr(self.local, "client"):
            self.local.client = Client()
        return self.local.client

    def upgrade(self):
        policy = {
            "origin": ["*"],
            "method": ["GET", "HEAD", "OPTIONS"],
            "responseHeader": ["*"],
            "maxAgeSeconds": self.TIMEOUT,
        }
        self.bucket.cors = [policy]
        self.bucket.update()

    def _locate_contenthash(self, content_hash):
        """Check if a file with the given hash exists on S3."""
        if content_hash is None:
            return
        prefix = path_prefix(content_hash)
        if prefix is None:
            return

        # First, check the standard file name:
        blob = Blob(os.path.join(prefix, "data"), self.bucket)
        if blob.exists():
            return blob

        # Second, iterate over all file names:
        for blob in self.client.list_blobs(self.bucket, max_results=1, prefix=prefix):
            return blob

    def _locate_key(self, key):
        if key is None:
            return
        blob = Blob(key, self.bucket)
        if blob.exists():
            return blob

    def archive_file(self, file_path, content_hash=None, mime_type=None):
        """Store the file located at the given path on Google, based on a path
        made up from its SHA1 content hash."""
        file_path = ensure_path(file_path)
        if content_hash is None:
            content_hash = checksum(file_path)

        if content_hash is None:
            return

        file_path = ensure_posix_path(file_path)
        for attempt in service_retries():
            try:
                # blob = self._locate_contenthash(content_hash)
                # if blob is not None:
                #     return content_hash

                path = os.path.join(path_prefix(content_hash), "data")
                blob = Blob(path, self.bucket)
                blob.upload_from_filename(file_path, content_type=mime_type)
                return content_hash
            except FAILURES:
                log.exception("Store error in GS")
                backoff(failures=attempt)

    def load_file(self, content_hash, file_name=None, temp_path=None):
        """Retrieve a file from Google storage and put it onto the local file
        system for further processing."""
        for attempt in service_retries():
            try:
                blob = self._locate_contenthash(content_hash)
                if blob is not None:
                    path = self._local_path(content_hash, file_name, temp_path)
                    blob.download_to_filename(path)
                    return path
            except FAILURES:
                log.exception("Load error in GS")
                backoff(failures=attempt)

        # Returns None for "persistent error" as well as "file not found" :/
        log.debug("[%s] not found, or the backend is down.", content_hash)

    def list_files(self, prefix=None):
        prefix = path_prefix(prefix)
        if prefix is None:
            return
        for blob in self.client.list_blobs(self.bucket, prefix=prefix):
            yield path_content_hash(blob.name)

    def _delete_blob(self, blob):
        for attempt in service_retries():
            try:
                blob.delete()
                return
            except NotFound:
                return
            except FAILURES:
                log.exception("Delete error in GS")
                backoff(failures=attempt)

    def delete_file(self, content_hash):
        """Check if a file with the given hash exists on S3."""
        if content_hash is None or len(content_hash) < HASH_LENGTH:
            return
        prefix = path_prefix(content_hash)
        if prefix is None:
            return

        # Iterate over all file names:
        for blob in self.client.list_blobs(self.bucket, prefix=prefix):
            self._delete_blob(blob)

    def generate_url(self, content_hash, file_name=None, mime_type=None, expire=None):
        blob = self._locate_contenthash(content_hash)
        if blob is None:
            return
        disposition = None
        if file_name is not None:
            disposition = "attachment; filename=%s" % file_name
        if expire is None:
            expire = datetime.utcnow() + timedelta(seconds=self.TIMEOUT)
        return blob.generate_signed_url(
            expire, response_type=mime_type, response_disposition=disposition
        )

    @property
    def can_publish(self):
        return True

    def publish_file(self, file_path, publish_path, mime_type=None):
        bucket = self.client.bucket(self._publication_bucket)
        blob = Blob(publish_path, bucket)
        blob.upload_from_filename(file_path, content_type=mime_type)
        blob.make_public(client=self.client)
        return blob.public_url
