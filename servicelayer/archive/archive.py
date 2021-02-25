import abc


class Archive(object):
    __metaclass__ = abc.ABCMeta

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

    @abc.abstractmethod
    def list_files(self, prefix=None):
        """List files in the archive within the given prefix. Must return an
        iterator of `content_hash`."""
        pass

    @abc.abstractmethod
    def delete_file(self, content_hash):
        pass

    def cleanup_file(self, content_hash, temp_path=None):
        pass

    def generate_url(self, content_hash, file_name=None, mime_type=None, expire=None):
        return None

    @property
    def can_publish(self):
        return False

    def publish_file(self, file_path, publish_path, mime_type=None):
        return None
