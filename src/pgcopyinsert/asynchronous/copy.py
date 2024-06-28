import typing as _t
import io as _io

import sqlalchemy.ext.asyncio as _sa_asyncio

import pgcopyinsert.drivers as _drivers


async def copy_from_csv(
    async_connection: _sa_asyncio.AsyncConnection,
    csv_file: _io.BytesIO,
    table_name: str,
    sep: str = ',',
    null: str = '',
    columns: _t.Optional[list[str]] = None,
    headers: bool = True,
    schema:_t.Optional[str] = None
) -> None:
    """
    Copy CSV file to PostgreSQL table.

    Example
    -------
    >>> from sqlalchemy.ext.asyncio import create_async_engine
    >>> from pgcopyinsert.asynchronous.copy import copy_from_csv

    >>> async_engine = sa.create_async_engine('postgresql+asyncpg://user:password@host:port/dbname')
    >>> async with async_engine.connect() as async_connection:
    ...     with open('people.csv', 'br') as csv_file:
    ...         await copy_from_csv(async_connection, csv_file, 'people')
    ...     await async_connection.commit()

    >>> await async_engine.dispose()
    """
    driver: str = _drivers.connection_driver_name(async_connection)
    if driver == 'psycopg':
        import pgcopyinsert.asynchronous.pg3.copy as _pg3_copy
        await _pg3_copy.copy_from_csv(async_connection, csv_file, table_name,
                                      sep, null, columns, headers, schema)
    elif driver == 'asyncpg':
        import pgcopyinsert.asynchronous.apg.copy as _apg_copy
        await _apg_copy.copy_from_csv(async_connection, csv_file, table_name,
                                      sep, null, columns, headers, schema)
    else:
        raise ValueError('driver must be psycopg of asyncpg')