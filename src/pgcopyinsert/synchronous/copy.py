import typing as _t
import io as _io

import sqlalchemy as _sa

import pgcopyinsert.drivers as _drivers


def copy_from_csv(
    connection: _sa.engine.base.Connection,
    csv_file: _io.BytesIO,
    table_name: str,
    sep: str = ',',
    null: str = '',
    columns: _t.Optional[list[str]] = None,
    headers: bool = True,
    schema: _t.Optional[str] = None
) -> None:
    driver: str = _drivers.connection_driver_name(connection)

    if driver == 'psycopg':
        import pgcopyinsert.synchronous.pg3.copy as _pg3_copy
        _pg3_copy.copy_from_csv(connection, csv_file, table_name, sep, null, columns, headers, schema)
    elif driver == 'pscopg2':
        import pgcopyinsert.synchronous.pg2.copy as _pg2_copy
        _pg2_copy.copy_from_csv(connection, csv_file, table_name, sep, null, columns, headers, schema)
    else:
        raise ValueError('driver must be psycopg or pscopg2')
    
    
