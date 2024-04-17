from unittest import TestCase
from urllib.parse import urlparse, parse_qs

from moto import mock_s3

from servicelayer.archive import init_archive
from servicelayer.archive.util import checksum, ensure_path


class S3ArchiveTest(TestCase):
    def setUp(self):
        self.mock = mock_s3()
        self.mock.start()
        self.archive = init_archive("s3", bucket="foo", publication_bucket="foo")
        self.file = ensure_path(__file__)

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
        checksum_ = "banana"
        out = self.archive.archive_file(self.file, checksum_)
        assert checksum_ == out, (checksum_, out)

    def test_generate_url(self):
        content_hash = self.archive.archive_file(self.file)
        url = self.archive.generate_url(content_hash)
        assert url is not None

        url = urlparse(url)
        assert url.netloc == "foo.s3.amazonaws.com"

    def test_generate_url_headers(self):
        content_hash = self.archive.archive_file(self.file)
        url = self.archive.generate_url(
            content_hash,
            file_name="test.txt",
            mime_type="text/plain",
        )
        assert url is not None

        url = urlparse(url)
        query = parse_qs(url.query)
        assert query["response-content-type"] == ["text/plain"]
        assert query["response-content-disposition"] == [
            "attachment; filename=test.txt"
        ]

    def test_publish_file(self):
        assert self.archive.can_publish
        url = self.archive.publish_file(self.file, "self.py", mime_type="text/python")
        assert "https://foo.s3.amazonaws.com/self.py" in url, url

    def test_load_file(self):
        out = self.archive.archive_file(self.file)
        path = self.archive.load_file(out)
        assert path is not None, path
        assert path.is_file(), path

    def test_cleanup_file(self):
        out = self.archive.archive_file(self.file)
        self.archive.cleanup_file(out)
        path = self.archive.load_file(out)
        assert path.is_file(), path
        self.archive.cleanup_file(out)
        assert not path.exists(), path

    def test_list_files(self):
        keys = list(self.archive.list_files())
        assert len(keys) == 0, keys
        out = self.archive.archive_file(self.file)
        keys = list(self.archive.list_files())
        assert len(keys) == 1, keys
        keys = list(self.archive.list_files(prefix=out[:4]))
        assert len(keys) == 1, keys
        assert keys[0] == out, keys
        keys = list(self.archive.list_files(prefix="banana"))
        assert len(keys) == 0, keys

    def test_delete_file(self):
        out = self.archive.archive_file(self.file)
        path = self.archive.load_file(out)
        assert path is not None, path
        self.archive.cleanup_file(out)
        self.archive.delete_file(out)
        path = self.archive.load_file(out)
        assert path is None, path
