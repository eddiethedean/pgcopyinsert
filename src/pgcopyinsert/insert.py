import sqlalchemy as sa
from sqlalchemy.dialects.postgresql.dml import Insert
from sqlalchemy.dialects.postgresql import insert


def insert_from_table_stmt(
    table1: sa.Table,
    table2: sa.Table
) -> Insert:
    return insert(table2).from_select(table1.columns.keys(), table1)


def insert_from_table_stmt_ocdn(
    table1: sa.Table,
    table2: sa.Table
) -> Insert:
    return insert_from_table_stmt(table1, table2).on_conflict_do_nothing()


def insert_from_table_stmt_ocdu(
    table1: sa.Table,
    table2: sa.Table
) -> Insert:
    return insert_from_table_stmt(table1, table2).on_conflict_do_update()