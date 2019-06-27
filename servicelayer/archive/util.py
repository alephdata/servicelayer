from hashlib import sha1
from pathlib import Path

BUF_SIZE = 1024 * 1024 * 16


def checksum(file_name):
    """Generate a hash for a given file name."""
    if file_name is not None:
        digest = sha1()
        with open(file_name, 'rb') as fh:
            while True:
                block = fh.read(BUF_SIZE)
                if not block:
                    break
                digest.update(block)
        return str(digest.hexdigest())


def ensure_path(file_path):
    if file_path is None or isinstance(file_path, Path):
        return file_path
    return Path(file_path).resolve()
