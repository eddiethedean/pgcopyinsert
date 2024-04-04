import typing as _t

from sqlalchemy.engine.interfaces import DBAPICursor


def copy_from_csv(
    cursor: DBAPICursor,
    csv_file,
    table_name: str,
    sep: str =',',
    null: str = '',
    columns: _t.Optional[list[str]] = None,
    headers=True,
    schema=None
) -> None:
    column_names: list[str] | None
    if headers:
        first_line: str = csv_file.readline().strip()
        column_names = first_line.split(sep) if columns is None else columns
    else:
        column_names = columns
    if schema:
        table_name = f'{schema}.{table_name}'
    cursor.copy_from(csv_file, table_name, sep=sep, null=null, columns=column_names)


