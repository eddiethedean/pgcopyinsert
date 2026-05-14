import io
from unittest.mock import MagicMock, patch

import pandas as pd
import polars as pl
import pytest

import pgcopyinsert.pd as pd_mod
import pgcopyinsert.pl as pl_mod
from pgcopyinsert.insert import insert_from_table_stmt_ocdn


def test_copyinsert_dataframe_writes_csv_and_delegates_to_copyinsert_csv() -> None:
    conn = MagicMock()
    df = pd.DataFrame({"x": [1], "y": [2]})
    with (
        patch("pgcopyinsert.pd._write.write_df_bytes_csv") as mock_write,
        patch("pgcopyinsert.pd._copyinsert.copyinsert_csv") as mock_ci,
    ):
        pd_mod.copyinsert_dataframe(
            df,
            "t",
            "t_tmp",
            conn,
            sep="|",
            schema="app",
            constraint="t_pkey",
            null="\\N",
        )

    mock_write.assert_called_once()
    w_args, _w_kw = mock_write.call_args
    assert w_args[0] is df
    assert isinstance(w_args[1], io.BytesIO)

    mock_ci.assert_called_once()
    pos, kw = mock_ci.call_args
    assert pos[0] is mock_write.call_args[0][1]  # same BytesIO passed through
    assert pos[1] == "t"
    assert pos[2] == "t_tmp"
    assert pos[3] is conn
    assert kw["sep"] == "|"
    assert kw["schema"] == "app"
    assert kw["columns"] == ["x", "y"]
    assert kw["headers"] is True
    assert kw["insert_function"] is insert_from_table_stmt_ocdn
    assert kw["constraint"] == "t_pkey"
    assert kw["null"] == "\\N"


def test_copyinsert_polars_writes_csv_and_delegates_to_copyinsert_csv() -> None:
    conn = MagicMock()
    df = pl.DataFrame({"a": [3], "b": [4]})
    with (
        patch.object(df, "write_csv") as mock_write_csv,
        patch("pgcopyinsert.pl._copyinsert.copyinsert_csv") as mock_ci,
    ):
        pl_mod.copyinsert_polars(
            df,
            "tbl",
            "tbl_tmp",
            conn,
            sep=";",
            schema=None,
            constraint="tbl_pkey",
            null="",
        )

    mock_write_csv.assert_called_once()
    assert mock_write_csv.call_args.kwargs["include_header"] is False
    assert mock_write_csv.call_args.kwargs["separator"] == ";"

    mock_ci.assert_called_once()
    pos, kw = mock_ci.call_args
    assert pos[1] == "tbl"
    assert pos[2] == "tbl_tmp"
    assert pos[3] is conn
    assert kw["headers"] is False
    assert kw["columns"] == ["a", "b"]
    assert kw["schema"] is None
    assert kw["insert_function"] is insert_from_table_stmt_ocdn
    assert kw["constraint"] == "tbl_pkey"
    assert kw["null"] == ""


def test_copyinsert_polars_deprecated_dataframe_emits_warning() -> None:
    conn = MagicMock()
    df = pl.DataFrame({"z": [1]})
    with (
        pytest.warns(DeprecationWarning, match="copyinsert_polars"),
        patch.object(df, "write_csv"),
        patch("pgcopyinsert.pl._copyinsert.copyinsert_csv"),
    ):
        pl_mod.copyinsert_dataframe(df, "t", "t_tmp", conn)
