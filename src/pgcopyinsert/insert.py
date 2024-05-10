import typing as _t

import sqlalchemy as _sa
import sqlalchemy.dialects.postgresql as _postgresql

InsertFunction = _t.Callable[[_sa.Table, _sa.Table, _t.Optional[str]], _sa.Insert]


def insert_from_table_stmt(
    table1: _sa.Table,
    table2: _sa.Table,
    constraint = None
) -> _postgresql.Insert:
    return _postgresql.insert(table2).from_select(table1.columns.keys(), table1)


def insert_from_table_stmt_ocdn(
    table1: _sa.Table,
    table2: _sa.Table,
    constraint: _t.Optional[str] = None
) -> _postgresql.dml.Insert:
    return insert_from_table_stmt(table1, table2).on_conflict_do_nothing(constraint=constraint)


def insert_from_table_stmt_ocdu(
    table1: _sa.Table,
    table2: _sa.Table,
    constraint: str
) -> _postgresql.dml.Insert:
    return insert_from_table_stmt(table1, table2).on_conflict_do_update(constraint=constraint)