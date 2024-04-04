import typing as _t

from contextlib import contextmanager

import sqlalchemy as sa
from sqlalchemy.engine.interfaces import DBAPICursor


@contextmanager
def raw_connection(engine: sa.engine.Engine, *args, **kwds) -> _t.Generator[sa.PoolProxiedConnection, None, None]:
    connection: sa.PoolProxiedConnection = engine.raw_connection(*args, **kwds)
    try:
        yield connection
    finally:
        connection.close()


@contextmanager
def raw_cursor(raw_connection: sa.PoolProxiedConnection) -> _t.Generator[DBAPICursor, None, None]:
    cursor: DBAPICursor = raw_connection.cursor()
    try:
        yield cursor
    finally:
        cursor.close()