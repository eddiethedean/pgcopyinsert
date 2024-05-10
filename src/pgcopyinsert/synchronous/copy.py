import typing as _t
import io as _io

import sqlalchemy as _sa

import pgcopyinsert.drivers as _drivers
import pgcopyinsert.synchronous.drivers as _sync_drivers


def copy_from_csv(
    connection: _sa.engine.base.Connection,
    csv_file: _io.BytesIO | _io.StringIO,
    table_name: str,
    sep: str = ',',
    null: str = '',
    columns: _t.Optional[list[str]] = None,
    headers: bool = True,
    schema: _t.Optional[str] = None
) -> None:
    driver: str = _drivers.connection_driver_name(connection)
    copy_function = _sync_drivers.driver_copy_from_csv(driver)
    driver_connection = _sync_drivers.get_driver_connection(connection)
    copy_function(
        driver_connection,
        csv_file, table_name,
        sep=sep, null=null,
        columns=columns, headers=headers,
        schema=schema
    )