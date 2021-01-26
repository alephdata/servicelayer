import os
import glob
import shutil
import logging
from normality import safe_filename

from servicelayer.archive.archive import Archive
from servicelayer.archive.util import ensure_path, checksum, BUF_SIZE
from servicelayer.archive.util import path_prefix, path_content_hash

log = logging.getLogger(__name__)


class FileArchive(Archive):
    def __init__(self, path=None):
        self.path = ensure_path(path)
        if self.path is None:
            raise ValueError("No archive path is set.")
        log.info("Archive: %s", self.path)

    def _locate_key(self, content_hash):
        prefix = path_prefix(content_hash)
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

        archive_prefix = path_prefix(content_hash)
        archive_path = self.path.joinpath(archive_prefix)
        archive_path.mkdir(parents=True, exist_ok=True)
        file_name = safe_filename(file_path, default="data")
        archive_path = archive_path.joinpath(file_name)
        with open(file_path, "rb") as fin:
            with open(archive_path, "wb") as fout:
                shutil.copyfileobj(fin, fout, BUF_SIZE)
        return content_hash

    def load_file(self, content_hash, file_name=None, temp_path=None):
        return self._locate_key(content_hash)

    def list_files(self, prefix=None):
        prefix = path_prefix(prefix)
        if prefix is None:
            prefix = ""
        path = self.path.joinpath(prefix)
        if path.is_dir():
            path = f"{path}/**/*"
        else:
            path = f"{path}*/**/*"
        for file_path in glob.iglob(path, recursive=True):
            if os.path.isfile(file_path):
                yield path_content_hash(file_path)

    def delete_file(self, content_hash):
        prefix = path_prefix(content_hash)
        if prefix is None:
            return
        path = self.path.joinpath(prefix)
        try:
            for file_name in path.iterdir():
                return file_name.unlink()
        except FileNotFoundError:
            return
