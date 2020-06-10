import os
import logging
import threading
from datetime import datetime, timedelta
from google.cloud.storage import Blob
from google.cloud.storage.client import Client
from google.api_core.exceptions import TooManyRequests, InternalServerError
from google.api_core.exceptions import ServiceUnavailable
from google.resumable_media.common import DataCorruption, InvalidResponse
from normality import safe_filename

from servicelayer.archive.virtual import VirtualArchive
from servicelayer.archive.util import checksum, ensure_path, ensure_posix_path
from servicelayer.util import service_retries, backoff

log = logging.getLogger(__name__)
FAILURES = (TooManyRequests, InternalServerError, ServiceUnavailable,
            DataCorruption, InvalidResponse)


class GoogleStorageArchive(VirtualArchive):
    TIMEOUT = 84600

    def __init__(self, bucket=None):
        super(GoogleStorageArchive, self).__init__(bucket)
        log.info("Archive: gs://%s", bucket)
        self._bucket = bucket
        self.local = threading.local()

    @property
    def bucket(self):
        if not hasattr(self.local, 'bucket'):
            client = Client()
            self.local.bucket = client.lookup_bucket(self._bucket)
            if self.bucket is None:
                self.local.bucket = client.create_bucket(self._bucket)
                self.upgrade()
        return self.local.bucket

    def upgrade(self):
        policy = {
            "origin": ['*'],
            "method": ['GET', 'HEAD', 'OPTIONS'],
            "responseHeader": ['*'],
            "maxAgeSeconds": self.TIMEOUT
        }
        self.bucket.cors = [policy]
        self.bucket.update()

    def _locate_contenthash(self, content_hash):
        """Check if a file with the given hash exists on S3."""
        if content_hash is None:
            return
        prefix = self._get_prefix(content_hash)
        if prefix is None:
            return

        # First, check the standard file name:
        blob = Blob(os.path.join(prefix, 'data'), self.bucket)
        if blob.exists():
            return blob

        # Second, iterate over all file names:
        for blob in self.bucket.list_blobs(max_results=1, prefix=prefix):
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

                path = os.path.join(self._get_prefix(content_hash), 'data')
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

    def generate_url(self, content_hash, file_name=None, mime_type=None):
        blob = self._locate_contenthash(content_hash)
        if blob is None:
            return
        disposition = None
        if file_name is not None:
            disposition = 'inline; filename=%s' % file_name
        expire = datetime.utcnow() + timedelta(seconds=self.TIMEOUT)
        return blob.generate_signed_url(expire,
                                        response_type=mime_type,
                                        response_disposition=disposition)

    def publish(self, namespace, file_path, mime_type=None):
        file_path = ensure_posix_path(file_path)
        file_name = safe_filename(file_path, default='data')
        store_path = '{0}/{1}'.format(namespace, file_name)

        for attempt in service_retries():
            try:
                blob = Blob(store_path, self.bucket)
                blob.upload_from_filename(file_path, content_type=mime_type)
            except FAILURES:
                log.exception("Store error in GS")
                backoff(failures=attempt)

    def delete_publication(self, namespace, file_name):
        key = '{0}/{1}'.format(namespace, file_name)
        for attempt in service_retries():
            try:
                blob = self._locate_key(key)
                if blob is not None:
                    blob.delete()
            except FAILURES:
                log.exception("Load error in GS")
                backoff(failures=attempt)

        log.warn("[%s] not found, or the backend is down.", key)

    def generate_publication_url(self, namespace, file_name, mime_type=None,
                                 expire=None):
        key = '{0}/{1}'.format(namespace, file_name)
        blob = self._locate_key(key)
        if blob is None:
            return
        disposition = 'attachment; filename=%s' % file_name
        expire = expire or self.TIMEOUT
        expire = datetime.utcnow() + timedelta(seconds=expire)
        return blob.generate_signed_url(expire,
                                        response_type=mime_type,
                                        response_disposition=disposition)
