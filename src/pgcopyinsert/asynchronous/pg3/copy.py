import typing as _t
import io as _io
import csv as _csv

import psycopg as _psycopg
import psycopg.sql as _sql

import pgcopyinsert.names as _names


def create_copy_query(table_name, fields) -> _sql.Composed:
    return _sql.SQL("COPY {table} ({fields}) FROM STDOUT").format(
        fields=_sql.SQL(',').join([_sql.Identifier(col) for col in fields]),
        table=_sql.Identifier(table_name)
    )


async def copy_from_csv(
    async_connection: _psycopg.AsyncConnection,
    csv_file: _io.TextIOWrapper,
    table_name: str,
    sep: str = ',',
    null: str = '',
    columns: _t.Optional[list[str]] = None,
    headers: bool = True,
    schema:_t.Optional[str] = None
) -> None:
    table_name, column_names = _names.adapt_names(csv_file, table_name, sep, columns, headers, schema)
    query: _sql.Composed = create_copy_query(table_name, column_names)

    async with async_connection.cursor() as async_cursor:
        async with async_cursor.copy(query) as copy:
            for row in _csv.reader(csv_file):
                values: list[str | None] = [None if val == null else val for val in row]
                await copy.write_row(values)