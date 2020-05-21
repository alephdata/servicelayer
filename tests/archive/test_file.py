import shutil
import tempfile
from unittest import TestCase

from servicelayer.archive import init_archive
from servicelayer.archive.util import checksum, ensure_path


class FileArchiveTest(TestCase):

    def setUp(self):
        tempdir = ensure_path(tempfile.gettempdir())
        self.path = tempdir.joinpath('sltest')
        self.archive = init_archive('file', path=self.path)
        self.file = ensure_path(__file__)

    def tearDown(self):
        if self.path.exists():
            shutil.rmtree(self.path)

    def test_basic_archive(self):
        checksum_ = checksum(self.file)
        assert checksum_ is not None, checksum_
        out = self.archive.archive_file(self.file)
        assert checksum_ == out, (checksum_, out)
        out2 = self.archive.archive_file(self.file)
        assert out == out2, (out, out2)

    def test_basic_archive_with_checksum(self):
        checksum_ = 'banana'
        out = self.archive.archive_file(self.file, checksum_)
        assert checksum_ == out, (checksum_, out)

    def test_generate_url(self):
        out = self.archive.archive_file(self.file)
        url = self.archive.generate_url(out)
        assert url is None, url

    def test_load_file(self):
        out = self.archive.archive_file(self.file)
        path = self.archive.load_file(out)
        assert path is not None, path
        assert self.path in path.parents
        assert path.is_file(), path

    def test_cleanup_file(self):
        out = self.archive.archive_file(self.file)
        self.archive.cleanup_file(out)
        path = self.archive.load_file(out)
        assert path.is_file(), path

    def test_publication(self):
        self.archive.publish('banana', self.file, 'text/plain')
        path = self.archive.load_publication('banana', 'test_file.py')
        assert path is not None, path
        assert path.is_file(), path
        assert path.name == 'test_file.py', path.name
        assert path.parent.name == 'banana', path.parent
        self.archive.delete_publication('banana', 'test_file.py')
        assert not path.exists()
