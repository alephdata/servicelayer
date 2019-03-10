import os
import shutil
import logging
from normality import safe_filename

from servicelayer.archive.archive import Archive
from servicelayer.archive.util import checksum, decode_path, BUF_SIZE

log = logging.getLogger(__name__)


class FileArchive(Archive):

    def __init__(self, path=None):
        self.path = decode_path(path)
        if self.path is None:
            raise ValueError('No archive path is set.')
        log.info("Archive: %s", self.path)

    def _locate_key(self, content_hash):
        prefix = self._get_prefix(content_hash)
        if prefix is None:
            return
        path = os.path.join(self.path, prefix)
        data_path = os.path.join(path, 'data')
        if os.path.exists(data_path):
            return data_path
        try:
            for file_name in os.listdir(path):
                return os.path.join(path, file_name)
        except OSError:
            return

    def archive_file(self, file_path, content_hash=None):
        """Import the given file into the archive."""
        if content_hash is None:
            content_hash = checksum(file_path)

        if self._locate_key(content_hash):
            return content_hash

        archive_prefix = self._get_prefix(content_hash)
        archive_path = os.path.join(self.path, archive_prefix)
        try:
            os.makedirs(archive_path)
        except Exception:
            return content_hash

        file_name = safe_filename(file_path, default='data')
        archive_path = os.path.join(archive_path, file_name)
        with open(file_path, 'rb') as fin:
            with open(archive_path, 'wb') as fout:
                shutil.copyfileobj(fin, fout, BUF_SIZE)
        return content_hash

    def load_file(self, content_hash, file_name=None, temp_path=None):
        return self._locate_key(content_hash)
