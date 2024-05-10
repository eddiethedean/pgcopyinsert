import typing as _t
import io as _io

import sqlalchemy.ext.asyncio as _sa_asyncio


async def get_driver_connection(
    async_connection: _sa_asyncio.AsyncConnection
) -> 'psycopg.AsyncConnection | asyncpg.Connection':
    raw_connection = await async_connection.get_raw_connection()
    return raw_connection.driver_connection


def driver_copy_from_csv(
    driver: str
) -> _t.Callable:
    if driver == 'psycopg':
        from pgcopyinsert.asynchronous.pg3.copy import copy_from_csv
    elif driver == 'asyncpg':
        from pgcopyinsert.asynchronous.apg.copy import copy_from_csv
    else:
        raise ValueError('connection string driver must be asyncpg or psycopg')
    return copy_from_csv


def get_driver_io(
    driver: str
) -> _t.Type[_io.BytesIO] | _t.Type[_io.StringIO]:
    if driver == 'psycopg':
        return _io.StringIO
    elif driver == 'asyncpg':
        return _io.BytesIO
    else:
        raise ValueError('connection string driver must be asyncpg or psycopg')