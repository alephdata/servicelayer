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
