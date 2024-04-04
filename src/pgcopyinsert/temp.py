import typing as _t

import sqlalchemy as sa


def create_temp_table_from_table(
    table: sa.Table,
    name: str,
    meta: sa.MetaData,
    columns: _t.Optional[list[str]] = None
) -> sa.Table:
    column_names: list[str] = [] if columns is None else columns
    temp_table = sa.Table(name, meta, prefixes=['TEMPORARY'])
    for col in table.c:
        if columns is None or col.name in column_names:
            temp_table.append_column(sa.Column(col.name, col.type))
    return temp_table
    
    
def create_table_stmt(
    table: sa.Table,
) -> sa.sql.ddl.CreateTable:
    return sa.schema.CreateTable(table)
    


