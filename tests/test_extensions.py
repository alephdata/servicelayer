from unittest import TestCase

from servicelayer.extensions import get_extensions


class ExtensionTest(TestCase):
    def test_extensions(self):
        # This relies on the servicelayer package being installed as `get_extensions`
        # ultimately reads entrypoints from the egg info
        exts = get_extensions("servicelayer.test")
        assert len(exts), exts
        assert get_extensions in exts, exts
