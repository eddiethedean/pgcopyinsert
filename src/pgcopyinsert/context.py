import typing as _t

from contextlib import contextmanager, asynccontextmanager

import sqlalchemy as sa
from sqlalchemy.engine.interfaces import DBAPICursor
from sqlalchemy.ext.asyncio import AsyncConnection
import asyncpg


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


@asynccontextmanager
async def async_raw_connection(async_connection: AsyncConnection) -> _t.AsyncGenerator[sa.PoolProxiedConnection, None]:
    connection: sa.PoolProxiedConnection = await async_connection.get_raw_connection()
    try:
        yield connection
    finally:
        connection.close()


@asynccontextmanager
async def asyncpg_connection(async_connection: AsyncConnection) -> _t.AsyncGenerator[asyncpg.Connection, _t.Any]:
    async with async_raw_connection(async_connection) as connection:
        asyncpg_con: asyncpg.Connection = connection.driver_connection # type: ignore
        try:
            yield asyncpg_con
        finally:
            await asyncpg_con.close()