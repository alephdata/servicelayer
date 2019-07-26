from unittest import TestCase
from datetime import datetime

from servicelayer.util import pack_datetime, pack_now, unpack_datetime
from servicelayer.util import pack_int, unpack_int
from servicelayer.util import dump_json, load_json


class UtilTest(TestCase):

    def test_int(self):
        packed = pack_int(555)
        unpacked = unpack_int(packed)
        assert unpacked == 555
        assert unpack_int('Banana') == 0

    def test_datetime(self):
        date = datetime.utcnow().date()
        packed = pack_datetime(date)
        unpacked = unpack_datetime(packed)
        assert unpacked == date
        now = pack_now()
        assert now.startswith(packed), now
        time = datetime.utcnow()
        packed = pack_datetime(time)
        unpacked = unpack_datetime(packed)
        assert unpacked == time
        assert pack_datetime('Banana') is None
        assert unpack_datetime('Banana') is None

    def test_object(self):
        data = {
            'help': 5,
            'xxx': datetime.utcnow(),
            'bla': None,
            'foo': set(['a']),
            'yum': b'xxx'
        }
        json = dump_json(data)
        out = load_json(json)
        assert 'bla' not in out, out
        assert 'xxx' in out, out
        assert 'a' in out['foo'], out
        assert load_json('') is None
        assert load_json(None) is None
        assert dump_json(None) == ''
