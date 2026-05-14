from unittest.mock import MagicMock, Mock, patch

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql.dml import Insert

from pgcopyinsert.insert import (
    insert_from_table_stmt,
    insert_from_table_stmt_ocdn,
    insert_from_table_stmt_ocdu,
)


@pytest.fixture
def mock_table1() -> MagicMock:
    return MagicMock(spec=sa.Table)


@pytest.fixture
def mock_table2() -> MagicMock:
    return MagicMock(spec=sa.Table)


def test_insert_from_table_stmt(mock_table1: MagicMock, mock_table2: MagicMock) -> None:
    result: Insert = insert_from_table_stmt(mock_table1, mock_table2)

    assert isinstance(result, Insert)
    assert result.table is mock_table2


def test_insert_from_table_stmt_ocdn(mock_table1: MagicMock, mock_table2: MagicMock) -> None:
    mock_insert = MagicMock(spec=Insert)
    mock_insert.on_conflict_do_nothing.return_value = Mock(spec=Insert)
    with patch(
        "pgcopyinsert.insert.insert_from_table_stmt",
        return_value=mock_insert,
    ) as mock_insert_from_table_stmt:
        result: Insert = insert_from_table_stmt_ocdn(mock_table1, mock_table2)

        assert result == mock_insert.on_conflict_do_nothing.return_value
        mock_insert_from_table_stmt.assert_called_once_with(mock_table1, mock_table2)


def test_insert_from_table_stmt_ocdu_compiles() -> None:
    meta = sa.MetaData()
    t1 = sa.Table("t1", meta, sa.Column("id", sa.Integer))
    t2 = sa.Table(
        "t2",
        meta,
        sa.Column("id", sa.Integer),
        sa.PrimaryKeyConstraint("id", name="t2_pkey"),
    )
    stmt = insert_from_table_stmt_ocdu(t1, t2, "t2_pkey")
    sql = str(stmt.compile(dialect=postgresql.dialect()))
    assert "ON CONFLICT" in sql.upper()
    assert "UPDATE" in sql.upper()
