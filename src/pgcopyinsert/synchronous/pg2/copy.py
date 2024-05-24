import typing as _t
import io as _io

import psycopg2._psycopg as _psycopg
import sqlalchemy as _sa
import pgcopyinsert.names as _names
import pgcopyinsert.synchronous.pg2.connection as _connection


def copy_from_csv(
    connection: _sa.connection,
    csv_file: _io.BytesIO,
    table_name: str,
    sep: str = ',',
    null: str = '',
    columns: _t.Optional[list[str]] = None,
    headers: bool = True,
    schema: _t.Optional[str] = None
) -> None:
    table_name, column_names = _names.adapt_names(csv_file, table_name, sep, columns, headers, schema)
    pg2_connection: _psycopg.connection = _connection.get_driver_connection(connection)
    cursor: _psycopg.cursor = pg2_connection.cursor()
    with _io.TextIOWrapper(csv_file, encoding='utf-8') as text_file:
        cursor.copy_from(text_file, table_name, sep=sep, null=null, columns=column_names)
