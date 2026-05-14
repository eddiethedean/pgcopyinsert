import io
from unittest.mock import MagicMock, patch

import sqlalchemy as sa

from pgcopyinsert.copyinsert import copyinsert_csv
from pgcopyinsert.insert import insert_from_table_stmt_ocdu


def _reflect_table(*, name: str = "target", col: str = "a"):
    def reflect(self, bind, schema=None, **kwargs):
        sa.Table(
            name,
            self,
            sa.Column(col, sa.Integer),
            schema=schema,
        )

    return reflect


def test_copyinsert_csv_runs_reflect_copy_insert_drop() -> None:
    """Orchestration: temp DDL, COPY, INSERT from temp, DROP — without a live DB."""

    conn = MagicMock()
    csv_file = io.BytesIO(b"a\n1\n")

    with (
        patch.object(sa.MetaData, "reflect", _reflect_table()),
        patch("pgcopyinsert.copyinsert._copy.copy_from_csv") as mock_copy,
        patch("pgcopyinsert.copyinsert._insert.insert_from_table_stmt_ocdn") as mock_insert_fn,
    ):
        mock_insert_fn.return_value = sa.text("INSERT INTO target SELECT * FROM target_tmp")
        copyinsert_csv(csv_file, "target", "target_tmp", conn, schema="public")

    mock_copy.assert_called_once()
    assert conn.execute.call_count == 3


def test_copyinsert_csv_forwards_copy_from_csv_arguments() -> None:
    conn = MagicMock()
    csv_file = io.BytesIO(b"x,y\n1,2\n")

    def reflect(self, bind, schema=None, **kwargs):
        sa.Table(
            "widgets",
            self,
            sa.Column("x", sa.Integer),
            sa.Column("y", sa.Integer),
            schema=schema,
        )

    with (
        patch.object(sa.MetaData, "reflect", reflect),
        patch("pgcopyinsert.copyinsert._copy.copy_from_csv") as mock_copy,
        patch(
            "pgcopyinsert.copyinsert._insert.insert_from_table_stmt_ocdn",
            return_value=sa.text("INSERT"),
        ),
    ):
        copyinsert_csv(
            csv_file,
            "widgets",
            "widgets_tmp",
            conn,
            sep=";",
            null="\\N",
            columns=["x", "y"],
            headers=False,
            schema="staging",
        )

    mock_copy.assert_called_once()
    args, kwargs = mock_copy.call_args
    assert args[0] is conn
    assert args[1] is csv_file
    assert args[2] == "widgets_tmp"
    assert kwargs == {
        "sep": ";",
        "null": "\\N",
        "columns": ["x", "y"],
        "headers": False,
        "schema": "staging",
    }


def test_copyinsert_csv_passes_constraint_to_custom_insert_function() -> None:
    conn = MagicMock()
    csv_file = io.BytesIO(b"id\n1\n")
    custom = MagicMock(return_value=sa.text("UPSERT"))

    with (
        patch.object(sa.MetaData, "reflect", _reflect_table(name="line", col="id")),
        patch("pgcopyinsert.copyinsert._copy.copy_from_csv"),
        patch(
            "pgcopyinsert.copyinsert._insert.insert_from_table_stmt_ocdn",
        ),
    ):
        copyinsert_csv(
            csv_file,
            "line",
            "line_tmp",
            conn,
            insert_function=custom,
            constraint="line_pkey",
        )

    custom.assert_called_once()
    _t1, _t2, cstr = custom.call_args[0]
    assert cstr == "line_pkey"


def test_copyinsert_csv_with_ocdu_uses_real_insert_statement() -> None:
    """Middle ``connection.execute`` receives a compiled PostgreSQL INSERT (upsert)."""
    conn = MagicMock()
    csv_file = io.BytesIO(b"id\n1\n")

    def reflect(self, bind, schema=None, **kwargs):
        sa.Table(
            "line",
            self,
            sa.Column("id", sa.Integer),
            sa.PrimaryKeyConstraint("id", name="line_pkey"),
            schema=schema,
        )

    with (
        patch.object(sa.MetaData, "reflect", reflect),
        patch("pgcopyinsert.copyinsert._copy.copy_from_csv"),
    ):
        copyinsert_csv(
            csv_file,
            "line",
            "line_tmp",
            conn,
            insert_function=insert_from_table_stmt_ocdu,
            constraint="line_pkey",
        )

    assert conn.execute.call_count == 3
    middle = conn.execute.call_args_list[1][0][0]
    sql = str(middle.compile(dialect=sa.dialects.postgresql.dialect()))
    assert "INSERT INTO line" in sql
    assert "ON CONFLICT" in sql.upper()
