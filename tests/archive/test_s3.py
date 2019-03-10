import os
from unittest import TestCase

from moto import mock_s3

from servicelayer.archive import init_archive
from servicelayer.archive.util import checksum


class FileArchiveTest(TestCase):

    def setUp(self):
        self.mock = mock_s3()
        self.mock.start()
        self.archive = init_archive('s3', bucket='foo')
        self.file = os.path.abspath(__file__)

    def tearDown(self):
        self.mock.stop()

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
        # assert False, url
        assert url is not None, url

    def test_load_file(self):
        out = self.archive.archive_file(self.file)
        path = self.archive.load_file(out)
        assert path is not None, path
        assert os.path.isfile(path), path

    def test_cleanup_file(self):
        out = self.archive.archive_file(self.file)
        self.archive.cleanup_file(out)
        path = self.archive.load_file(out)
        assert os.path.isfile(path), path
        self.archive.cleanup_file(out)
        assert not os.path.isfile(path), path
