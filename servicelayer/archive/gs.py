import os
import logging
from datetime import datetime, timedelta
from google.cloud.storage import Blob
from google.cloud.storage.client import Client

from servicelayer.archive.virtual import VirtualArchive
from servicelayer.archive.util import checksum

log = logging.getLogger(__name__)


class GoogleStorageArchive(VirtualArchive):
    TIMEOUT = 84600

    def __init__(self, bucket=None):
        super(GoogleStorageArchive, self).__init__(bucket)
        self.client = Client()
        log.info("Archive: gs://%s", bucket)

        self.bucket = self.client.lookup_bucket(bucket)
        if self.bucket is None:
            self.bucket = self.client.create_bucket(bucket)

        policy = {
            "origin": ['*'],
            "method": ['GET'],
            "responseHeader": [
                'Accept-Ranges',
                'Content-Encoding',
                'Content-Length',
                'Content-Range'
            ],
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
        if blob.exists(self.client):
            return blob

        # Second, iterate over all file names:
        for blob in self.bucket.list_blobs(max_results=1, prefix=prefix):
            return blob

    def archive_file(self, file_path, content_hash=None):
        """Store the file located at the given path on S3, based on a path
        made up from its SHA1 content hash."""
        if content_hash is None:
            content_hash = checksum(file_path)

        blob = self._locate_blob(content_hash)
        if blob is None:
            path = os.path.join(self._get_prefix(content_hash), 'data')
            blob = Blob(path, self.bucket)
            blob.upload_from_filename(file_path)
        return content_hash

    def load_file(self, content_hash, file_name=None, temp_path=None):
        """Retrieve a file from S3 storage and put it onto the local file
        system for further processing."""
        blob = self._locate_blob(content_hash)
        if blob is not None:
            path = self._local_path(content_hash, file_name, temp_path)
            blob.download_to_filename(path)
            return path

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
