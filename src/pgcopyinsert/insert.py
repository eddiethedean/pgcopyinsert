import typing as _t

import sqlalchemy as _sa
import sqlalchemy.dialects.postgresql as _postgresql

InsertFunction = _t.Callable[[_sa.Table, _sa.Table, str | None], _sa.Insert]


def insert_from_table_stmt(
    table1: _sa.Table,
    table2: _sa.Table,
) -> _postgresql.Insert:
    return _postgresql.insert(table2).from_select(table1.columns.keys(), table1)


def insert_from_table_stmt_ocdn(
    table1: _sa.Table, table2: _sa.Table, constraint: str | None = None
) -> _postgresql.dml.Insert:
    return insert_from_table_stmt(table1, table2).on_conflict_do_nothing(constraint=constraint)


def insert_from_table_stmt_ocdu(
    table1: _sa.Table,
    table2: _sa.Table,
    constraint: str,
) -> _postgresql.dml.Insert:
    stmt = insert_from_table_stmt(table1, table2)
    excluded = stmt.excluded
    set_ = {col.name: excluded[col.name] for col in table2.c}
    return stmt.on_conflict_do_update(constraint=constraint, set_=set_)
