import os
import logging
from datetime import datetime, timedelta
from google.cloud.storage import Blob
from google.cloud.storage.client import Client
from google.api_core.exceptions import TooManyRequests, InternalServerError
from google.api_core.exceptions import ServiceUnavailable
from google.resumable_media.common import DataCorruption, InvalidResponse

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
        self.client = Client()
        log.info("Archive: gs://%s", bucket)

        self.bucket = self.client.lookup_bucket(bucket)
        if self.bucket is None:
            self.bucket = self.client.create_bucket(bucket)
            self.upgrade()

    def upgrade(self):
        # 'Accept-Ranges',
        # 'Content-Encoding',
        # 'Content-Length',
        # 'Content-Range',
        # 'Cache-Control',
        # 'Content-Language',
        # 'Content-Type',
        # 'Expires',
        # 'Last-Modified',
        # 'Pragma',
        # 'Range',
        # 'Date',
        policy = {
            "origin": ['*'],
            "method": ['GET', 'HEAD', 'OPTIONS'],
            "responseHeader": ['*'],
            "maxAgeSeconds": self.TIMEOUT
        }
        self.bucket.cors = [policy]
        self.bucket.update()

    def _locate_blob(self, content_hash):
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
                # blob = self._locate_blob(content_hash)
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
                blob = self._locate_blob(content_hash)
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
        blob = self._locate_blob(content_hash)
        if blob is None:
            return
        disposition = None
        if file_name is not None:
            disposition = 'inline; filename=%s' % file_name
        expire = datetime.utcnow() + timedelta(seconds=self.TIMEOUT)
        return blob.generate_signed_url(expire,
                                        response_type=mime_type,
                                        response_disposition=disposition)
