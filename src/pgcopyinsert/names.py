import typing as _t


def adapt_names(
    csv_file,
    table_name: str,
    sep: str,
    columns: _t.Optional[list[str]],
    headers: bool,
    schema: _t.Optional[str]
) -> tuple[str, list[str]]:
    column_names: list[str] | None
    if headers:
        first_line: str = csv_file.readline().strip()
        column_names = first_line.split(sep) if columns is None else columns
    else:
        column_names = columns
    if schema:
        table_name = f'{schema}.{table_name}'
    return table_name, column_names