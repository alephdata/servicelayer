import os
import abc


class Archive(object):
    __metaclass__ = abc.ABCMeta

    def _get_prefix(self, content_hash):
        if content_hash is not None:
            return os.path.join(content_hash[:2],
                                content_hash[2:4],
                                content_hash[4:6],
                                content_hash)

    def upgrade(self):
        """Run maintenance on the store."""
        pass

    @abc.abstractmethod
    def archive_file(self, file_path, content_hash=None, mime_type=None):
        """Import the given file into the archive."""
        pass

    @abc.abstractmethod
    def load_file(self, content_hash, file_name=None, temp_path=None):
        pass

    def cleanup_file(self, content_hash, temp_path=None):
        pass

    def generate_url(self, content_hash, file_name=None, mime_type=None):
        return None
