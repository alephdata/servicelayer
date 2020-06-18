import logging
from datetime import datetime
from sqlalchemy import Column, DateTime, String
from sqlalchemy import Table, MetaData, JSON
from sqlalchemy import create_engine, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import insert as upsert

from servicelayer import settings

log = logging.getLogger(__name__)


class Tags(object):
    """Create persistent cached tags in a SQL database backend. This is
    a heavy-duty replacement for the simpler redis cache."""

    def __init__(self, name, uri=settings.TAGS_DATABASE_URI, **config):
        self.name = name
        self.engine = create_engine(uri, **config)
        self.is_postgres = self.engine.dialect.name == 'postgresql'
        self.table = Table(name, MetaData(self.engine),
            Column('key', String, primary_key=True),  # noqa
            Column('value', JSONB if self.is_postgres else JSON),
            Column('timestamp', DateTime),
            extend_existing=True
        )
        self.table.create(bind=self.engine, checkfirst=True)

    def delete(self, key=None, prefix=None):
        stmt = self.table.delete()
        if key is not None:
            stmt = stmt.where(self.table.c.key == key)
        if prefix is not None:
            stmt = stmt.where(self.table.c.key.startswith(prefix))
        self.engine.execute(stmt)

    def close(self):
        self.engine.dispose()

    def get(self, key, since=None):
        stmt = select([self.table.c.value])
        stmt = stmt.where(self.table.c.key == key)
        if since is not None:
            stmt = stmt.where(self.table.c.timestamp >= since)
        rp = self.engine.execute(stmt)
        row = rp.fetchone()
        if row is not None:
            return row.value

    def exists(self, key, since=None):
        stmt = select([func.count()])
        stmt = stmt.where(self.table.c.key == key)
        if since is not None:
            stmt = stmt.where(self.table.c.timestamp >= since)
        rp = self.engine.execute(stmt)
        count = rp.scalar()
        return count > 0

    def _store_values(self, conn, row):
        try:
            stmt = self.table.insert().values(row)
            conn.execute(stmt)
        except IntegrityError:
            changing = ('value', 'timestamp',)
            changed = {c: row.get(c, {}) for c in changing}
            stmt = self.table.update().values(changed)
            stmt = stmt.where(self.table.c.key == row['key'])
            conn.execute(stmt)

    def _upsert_values(self, conn, row):
        """Use postgres' upsert mechanism (ON CONFLICT TO UPDATE)."""
        istmt = upsert(self.table).values(row)
        stmt = istmt.on_conflict_do_update(
            index_elements=['key'],
            set_=dict(
                value=istmt.excluded.value,
                timestamp=istmt.excluded.timestamp,
            )
        )
        conn.execute(stmt)

    def set(self, key, value):
        conn = self.engine.connect()
        tx = conn.begin()
        now = datetime.utcnow()
        row = {'key': key, 'value': value, 'timestamp': now}
        try:
            if self.is_postgres:
                self._upsert_values(conn, row)
            else:
                self._store_values(conn, row)
            tx.commit()
        except Exception:
            tx.rollback()
            self.close()
            log.exception("Database error")

    def __repr__(self):
        return '<Tags(%r, %r)>' % (self.engine, self.name)
