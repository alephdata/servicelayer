import pytest
from unittest import TestCase

from servicelayer.archive.util import sanitize_checksum


class UtilTest(TestCase):
    def test_sanitize_checksum(self):
        assert sanitize_checksum("0123456789abcdef") == "0123456789abcdef"

        with pytest.raises(ValueError, match="Checksum is empty"):
            sanitize_checksum(None)

        with pytest.raises(ValueError, match="Checksum is empty"):
            sanitize_checksum("")

        with pytest.raises(ValueError, match='Checksum contains invalid character "n"'):
            sanitize_checksum("banana")

        with pytest.raises(ValueError, match='Checksum contains invalid character "/"'):
            sanitize_checksum("/")
