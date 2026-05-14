import io
from unittest.mock import MagicMock, patch

import sqlalchemy as sa

from pgcopyinsert.copyinsert import copyinsert_csv


def test_copyinsert_csv_runs_reflect_copy_insert_drop() -> None:
    """Orchestration: temp DDL, COPY, INSERT from temp, DROP — without a live DB."""

    def reflect(self, bind, schema=None, **kwargs):
        sa.Table(
            "target",
            self,
            sa.Column("a", sa.Integer),
            schema=schema,
        )

    conn = MagicMock()
    csv_file = io.BytesIO(b"a\n1\n")

    with (
        patch.object(sa.MetaData, "reflect", reflect),
        patch("pgcopyinsert.copyinsert._copy.copy_from_csv") as mock_copy,
        patch("pgcopyinsert.copyinsert._insert.insert_from_table_stmt_ocdn") as mock_insert_fn,
    ):
        mock_insert_fn.return_value = sa.text("INSERT INTO target SELECT * FROM target_tmp")
        copyinsert_csv(csv_file, "target", "target_tmp", conn, schema="public")

    mock_copy.assert_called_once()
    assert conn.execute.call_count == 3
