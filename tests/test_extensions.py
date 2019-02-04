from unittest import TestCase

from servicelayer.extensions import get_extensions


class ExtensionTest(TestCase):

    def test_extensions(self):
        exts = get_extensions('servicelayer.test')
        assert len(exts), exts
        assert get_extensions in exts, exts
