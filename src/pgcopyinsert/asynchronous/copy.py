import typing as _t
import io as _io

import sqlalchemy.ext.asyncio as _sa_asyncio

import pgcopyinsert.drivers as _drivers
import pgcopyinsert.asynchronous.drivers as _async_drivers


async def copy_from_csv(
    async_connection: _sa_asyncio.AsyncConnection,
    csv_file: _io.TextIOWrapper,
    table_name: str,
    sep: str = ',',
    null: str = '',
    columns: _t.Optional[list[str]] = None,
    headers: bool = True,
    schema:_t.Optional[str] = None
) -> None:
    driver: str = _drivers.connection_driver_name(async_connection)
    copy_function = _async_drivers.driver_copy_from_csv(driver)
    async_driver_connection = await _async_drivers.get_driver_connection(async_connection)
    await copy_function(
        async_driver_connection,
        csv_file,
        table_name,
        sep=sep, null=null, columns=columns,
        headers=headers, schema=schema
    )