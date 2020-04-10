import time

from servicelayer.settings import REDIS_PREFIX as PREFIX
from servicelayer.cache import make_key


class RateLimit(object):
    """Limit the rate of stages on a given resource during a
    stated interval."""

    def __init__(self, conn, resource, limit=100, interval=60, unit=1):
        self.conn = conn
        self.resource = resource
        self.limit = max(0.1, limit)
        self.interval = max(1, interval)
        self.unit = unit

    def _time(self):
        return int(time.time() / self.unit)

    def _keys(self):
        base = self._time()
        for slot in range(base, base + self.interval):
            yield make_key(PREFIX, 'rate', self.resource, slot)

    def update(self, amount=1):
        """Set the cached counts for stats keeping."""
        pipe = self.conn.pipeline()
        for key in self._keys():
            pipe.incr(key, amount=amount)
            pipe.expire(key, (self.interval * self.unit) + 2)
        values = pipe.execute()[::2]
        return (sum(values) / self.interval)

    def check(self):
        """Check if the resource has exceeded the rate limit."""
        key = make_key(PREFIX, 'rate', self.resource, self._time())
        count = int(self.conn.get(key) or 0)
        return count < self.limit

    def comply(self, amount=1):
        """Update, then sleep for the time required to adhere to the
        rate limit."""
        expected_interval = (self.interval * self.unit) / self.limit
        key = make_key(PREFIX, 'rate', self.resource, self._time())
        count = int(self.conn.get(key) or 0)
        if count != 0:
            avg_interval = (self.interval * self.unit) / (count + 1)
            excess = expected_interval - avg_interval
            if excess >= 0:
                time.sleep(expected_interval)
        self.update(amount=amount)
