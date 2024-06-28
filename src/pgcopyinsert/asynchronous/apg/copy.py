import typing as _t
import io as _io

import sqlalchemy.ext.asyncio as _sa_asyncio
import asyncpg as _asyncpg
import pgcopyinsert.asynchronous.apg.connection as _connection



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
    >>> from pgcopyinsert.asynchronous.apg.copy import copy_from_csv

    >>> async_engine = sa.create_async_engine('postgresql+asyncpg://user:password@host:port/dbname')
    >>> async with async_engine.connect() as async_connection:
    ...     with open('people.csv', 'br') as csv_file:
    ...         await copy_from_csv(async_connection, csv_file, 'people')
    ...     await async_connection.commit()

    >>> await async_engine.dispose()
    """
    apg_async_connection: _asyncpg.Connection
    apg_async_connection = await _connection.get_driver_connection(async_connection)
    await apg_async_connection.copy_to_table(
            table_name, source=csv_file, delimiter=sep,
            header=headers, null=null, columns=columns,
            schema_name=schema, format='csv'
        )