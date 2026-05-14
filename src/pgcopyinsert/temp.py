import sqlalchemy as _sa


def create_temp_table_from_table(
    table: _sa.Table, name: str, meta: _sa.MetaData, columns: list[str] | None = None
) -> _sa.Table:
    column_names: list[str] = [] if columns is None else columns
    if columns is not None and len(column_names) == 0:
        raise ValueError(
            "columns= must be omitted (all columns) or a non-empty list of column names; "
            "an empty list would create a temp table with no columns."
        )
    temp_table = _sa.Table(name, meta, prefixes=["TEMPORARY"])
    for col in table.c:
        if columns is None or col.name in column_names:
            temp_table.append_column(_sa.Column(col.name, col.type), replace_existing=True)
    return temp_table


def create_table_stmt(
    table: _sa.Table,
) -> _sa.sql.ddl.CreateTable:
    return _sa.schema.CreateTable(table)
