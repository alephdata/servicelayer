from unittest import TestCase

from servicelayer.util import service_retries


class BasicTest(TestCase):

    def test_basic(self):
        assert len(list(service_retries()))
