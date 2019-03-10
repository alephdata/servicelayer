import os
import shutil
import tempfile
from unittest import TestCase

from servicelayer.archive import init_archive
from servicelayer.archive.util import checksum


class FileArchiveTest(TestCase):

    def setUp(self):
        self.path = os.path.join(tempfile.gettempdir(), 'storagelayer_test')
        self.archive = init_archive('file', path=self.path)
        self.file = os.path.abspath(__file__)

    def tearDown(self):
        if os.path.exists(self.path):
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
        assert path.startswith(self.path)
        assert os.path.isfile(path), path

    def test_cleanup_file(self):
        out = self.archive.archive_file(self.file)
        self.archive.cleanup_file(out)
        path = self.archive.load_file(out)
        assert os.path.isfile(path), path
