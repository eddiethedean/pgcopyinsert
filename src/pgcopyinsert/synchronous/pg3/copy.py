import typing as _t
from io import TextIOWrapper
import csv

import psycopg.sql as _sql
from psycopg.cursor import Cursor

from pgcopyinsert.names import adapt_names


def create_copy_query(table_name, fields) -> _sql.Composed:
    return _sql.SQL("COPY {table} ({fields}) FROM STDOUT").format(
        fields=_sql.SQL(',').join([_sql.Identifier(col) for col in fields]),
        table=_sql.Identifier(table_name)
    )


def copy_from_csv(
    cursor: Cursor,
    csv_file: TextIOWrapper,
    table_name: str,
    sep: str = ',',
    null: str = '',
    columns: _t.Optional[list[str]] = None,
    headers: bool = True,
    schema: _t.Optional[str] = None
) -> None:
    table_name, column_names = adapt_names(csv_file, table_name, sep, columns, headers, schema)
    query = create_copy_query(table_name, column_names)
    with cursor.copy(query) as copy:
        for row in csv.reader(csv_file):
            values = [None if val == null else val for val in row]
            copy.write_row(values)