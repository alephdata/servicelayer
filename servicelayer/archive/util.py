import os
from hashlib import sha1
from pathlib import Path

BUF_SIZE = 1024 * 1024 * 16
HASH_LENGTH = 40  # sha1


def ensure_path(file_path):
    if file_path is None or isinstance(file_path, Path):
        return file_path
    return Path(file_path).resolve()


def ensure_posix_path(file_path):
    if isinstance(file_path, Path):
        file_path = file_path.as_posix()
    return file_path


def checksum(file_name):
    """Generate a hash for a given file name."""
    file_name = ensure_path(file_name)
    if file_name is not None and file_name.is_file():
        digest = sha1()
        with open(file_name, "rb") as fh:
            while True:
                block = fh.read(BUF_SIZE)
                if not block:
                    break
                digest.update(block)
        return str(digest.hexdigest())


def path_prefix(content_hash):
    """Get a prefix for a content hashed folder structure."""
    if content_hash is None:
        return None
    prefix = os.path.join(content_hash[:2], content_hash[2:4], content_hash[4:6])
    if len(content_hash) >= 6:
        prefix = os.path.join(prefix, content_hash)
    return prefix.rstrip(os.path.sep)


def path_content_hash(path):
    _, content_hash, _ = ensure_posix_path(path).rsplit(os.path.sep, 2)
    return content_hash
