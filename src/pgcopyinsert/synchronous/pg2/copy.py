import typing as _t

#from psycopg2.cursor import Cursor
from pgcopyinsert.names import adapt_names


def copy_from_csv(
    cursor: 'psycopg2.Cursor',
    csv_file,
    table_name: str,
    sep: str = ',',
    null: str = '',
    columns: _t.Optional[list[str]] = None,
    headers: bool = True,
    schema: _t.Optional[str] = None
) -> None:
    table_name, column_names = adapt_names(csv_file, table_name, sep, columns, headers, schema)
    cursor.copy_from(csv_file, table_name, sep=sep, null=null, columns=column_names)
