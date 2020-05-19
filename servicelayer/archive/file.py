import shutil
import logging
from normality import safe_filename

from servicelayer.archive.archive import Archive
from servicelayer.archive.util import ensure_path, checksum, BUF_SIZE

log = logging.getLogger(__name__)


class FileArchive(Archive):

    def __init__(self, path=None):
        self.path = ensure_path(path)
        if self.path is None:
            raise ValueError('No archive path is set.')
        log.info("Archive: %s", self.path)

    def _locate_key(self, content_hash):
        prefix = self._get_prefix(content_hash)
        if prefix is None:
            return
        path = self.path.joinpath(prefix)
        try:
            for file_name in path.iterdir():
                return file_name.resolve()
        except FileNotFoundError:
            return

    def archive_file(self, file_path, content_hash=None, mime_type=None):
        """Import the given file into the archive."""
        if content_hash is None:
            content_hash = checksum(file_path)

        if content_hash is None:
            return

        if self._locate_key(content_hash):
            return content_hash

        archive_prefix = self._get_prefix(content_hash)
        archive_path = self.path.joinpath(archive_prefix)
        archive_path.mkdir(parents=True, exist_ok=True)
        file_name = safe_filename(file_path, default='data')
        archive_path = archive_path.joinpath(file_name)
        with open(file_path, 'rb') as fin:
            with open(archive_path, 'wb') as fout:
                shutil.copyfileobj(fin, fout, BUF_SIZE)
        return content_hash

    def load_file(self, content_hash, file_name=None, temp_path=None):
        return self._locate_key(content_hash)

    def publish(self, namespace, file_path, mime_type=None):
        store_path = self.path.joinpath(namespace)
        store_path.mkdir(parents=True, exist_ok=True)
        file_name = safe_filename(file_path, default='data')
        store_path = store_path.joinpath(file_name)
        with open(file_path, 'rb') as fin:
            with open(store_path, 'wb') as fout:
                shutil.copyfileobj(fin, fout, BUF_SIZE)

    def load_publication(self, namespace, file_name, temp_path=None):
        path = self.path.joinpath(namespace, file_name)
        try:
            return path.resolve(strict=True)
        except FileNotFoundError:
            return

    def delete_publication(self, namespace, file_name):
        path = self.path.joinpath(namespace, file_name)
        try:
            path.unlink()
        except FileNotFoundError:
            pass
