import sys
from hashlib import sha1

BUF_SIZE = 1024 * 1024 * 16


def checksum(file_name):
    """Generate a hash for a given file name."""
    digest = sha1()
    with open(file_name, 'rb') as fh:
        while True:
            block = fh.read(BUF_SIZE)
            if not block:
                break
            digest.update(block)
    return str(digest.hexdigest())


def decode_path(file_path):
    """Decode a path specification into unicode."""
    if file_path is None:
        return
    if isinstance(file_path, bytes):
        file_path = file_path.decode(sys.getfilesystemencoding())
    return file_path
