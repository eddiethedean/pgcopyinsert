import pytest
import sqlalchemy as sa

from pgcopyinsert.temp import create_table_stmt, create_temp_table_from_table


@pytest.fixture
def test_tables() -> tuple[sa.Table, sa.Table]:
    metadata = sa.MetaData()
    test_table = sa.Table(
        "test_table",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
    )
    temp_test_table = sa.Table("temp_test_table", metadata, sa.Column("id", sa.Integer))
    return test_table, temp_test_table


def test_create_temp_table_from_table(test_tables: tuple[sa.Table, sa.Table]) -> None:
    test_table, _ = test_tables
    metadata = sa.MetaData()
    result: sa.Table = create_temp_table_from_table(test_table, "temp_test_table", metadata)
    assert result.name == "temp_test_table"
    assert list(result.c.keys()) == ["id"]
    ddl = str(create_table_stmt(result).compile(dialect=sa.dialects.postgresql.dialect()))
    assert "TEMPORARY" in ddl.upper()


def test_create_table_stmt(test_tables: tuple[sa.Table, sa.Table]) -> None:
    test_table, _ = test_tables
    metadata = sa.MetaData()
    temp = create_temp_table_from_table(test_table, "temp_test_table", metadata)
    stmt = create_table_stmt(temp)
    sql = str(stmt.compile(dialect=sa.dialects.postgresql.dialect()))
    assert "CREATE" in sql.upper()
    assert "TEMP" in sql.upper()