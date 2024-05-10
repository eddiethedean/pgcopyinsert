import typing as _t

import sqlalchemy as _sa


def create_temp_table_from_table(
    table: _sa.Table,
    name: str,
    meta: _sa.MetaData,
    columns: _t.Optional[list[str]] = None
) -> _sa.Table:
    column_names: list[str] = [] if columns is None else columns
    temp_table = _sa.Table(name, meta, prefixes=['TEMPORARY'])
    for col in table.c:
        if columns is None or col.name in column_names:
            temp_table.append_column(_sa.Column(col.name, col.type), replace_existing=True)
    return temp_table


def create_table_stmt(
    table: _sa.Table,
) -> _sa.sql.ddl.CreateTable:
    return _sa.schema.CreateTable(table)



