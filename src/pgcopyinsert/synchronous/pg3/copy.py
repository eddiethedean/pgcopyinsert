import typing as _t
import io as _io
import csv as _csv

import sqlalchemy as _sa
import psycopg.cursor as _pg2_cursor
import psycopg.connection as _pg3_connection
import psycopg.sql as _sql

import pgcopyinsert.synchronous.pg3.connection as _connection
import pgcopyinsert.query as _query
import pgcopyinsert.names as _names


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
    table_name, column_names = _names.adapt_names(csv_file, table_name, sep, columns, headers, schema)
    query: _sql.Composed = _query.create_copy_query(table_name, column_names)
    pg3_connection: _pg3_connection.Connection = _connection.get_driver_connection(connection)
    cursor: _pg2_cursor.Cursor = pg3_connection.cursor()
    with cursor.copy(query) as copy:
        with _io.TextIOWrapper(csv_file, encoding='utf-8') as text_file:
            for row in _csv.reader(text_file):
                values: list[str | None] = [None if val == null else val for val in row]
                copy.write_row(values)