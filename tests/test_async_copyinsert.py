import io
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import polars as pl
import pytest
import sqlalchemy as sa

from pgcopyinsert.asynchronous.copyinsert import copyinsert_csv


@pytest.mark.asyncio
async def test_async_copyinsert_csv_runs_reflect_copy_insert_drop() -> None:
    def reflect(self, bind, schema=None, **kwargs):
        sa.Table(
            "target",
            self,
            sa.Column("a", sa.Integer),
            schema=schema,
        )

    async_conn = MagicMock()
    async_conn.execute = AsyncMock()

    async def fake_run_sync(fn, *args, **kwargs):
        return fn(MagicMock(), *args, **kwargs)

    async_conn.run_sync = AsyncMock(side_effect=fake_run_sync)

    csv_file = io.BytesIO(b"a\n1\n")
    with (
        patch.object(sa.MetaData, "reflect", reflect),
        patch(
            "pgcopyinsert.asynchronous.copyinsert._copy.copy_from_csv",
            new_callable=AsyncMock,
        ) as mock_copy,
        patch(
            "pgcopyinsert.asynchronous.copyinsert._insert.insert_from_table_stmt_ocdn",
        ) as mock_insert_fn,
    ):
        mock_insert_fn.return_value = sa.text("INSERT INTO target SELECT * FROM target_tmp")
        await copyinsert_csv(csv_file, "target", "target_tmp", async_conn, schema="public")

    mock_copy.assert_awaited_once()
    assert async_conn.execute.await_count == 3


@pytest.mark.asyncio
async def test_async_copyinsert_csv_passes_temp_table_name_to_copy() -> None:
    def reflect(self, bind, schema=None, **kwargs):
        sa.Table("orders", self, sa.Column("id", sa.Integer), schema=schema)

    async_conn = MagicMock()
    async_conn.execute = AsyncMock()

    async def fake_run_sync(fn, *args, **kwargs):
        return fn(MagicMock(), *args, **kwargs)

    async_conn.run_sync = AsyncMock(side_effect=fake_run_sync)

    csv_file = io.BytesIO(b"id\n9\n")
    with (
        patch.object(sa.MetaData, "reflect", reflect),
        patch(
            "pgcopyinsert.asynchronous.copyinsert._copy.copy_from_csv",
            new_callable=AsyncMock,
        ) as mock_copy,
        patch(
            "pgcopyinsert.asynchronous.copyinsert._insert.insert_from_table_stmt_ocdn",
            return_value=sa.text("INSERT"),
        ),
    ):
        await copyinsert_csv(
            csv_file,
            "orders",
            "orders_staging",
            async_conn,
            columns=["id"],
            headers=True,
            schema="sales",
        )

    assert mock_copy.await_args.args[2] == "orders_staging"
    assert mock_copy.await_args.kwargs["schema"] == "sales"
    assert mock_copy.await_args.kwargs["columns"] == ["id"]


@pytest.mark.asyncio
async def test_async_copyinsert_dataframe_delegates() -> None:
    from pgcopyinsert.asynchronous import pd as async_pd

    df = pd.DataFrame({"k": [1]})
    async_conn = MagicMock()
    async_conn.execute = AsyncMock()

    async def fake_run_sync(fn, *args, **kwargs):
        return fn(MagicMock(), *args, **kwargs)

    async_conn.run_sync = AsyncMock(side_effect=fake_run_sync)

    def reflect(self, bind, schema=None, **kwargs):
        sa.Table("t", self, sa.Column("k", sa.Integer), schema=schema)

    with (
        patch.object(sa.MetaData, "reflect", reflect),
        patch("pgcopyinsert.asynchronous.pd._write.write_df_bytes_csv"),
        patch(
            "pgcopyinsert.asynchronous.copyinsert._copy.copy_from_csv",
            new_callable=AsyncMock,
        ),
        patch(
            "pgcopyinsert.asynchronous.copyinsert._insert.insert_from_table_stmt_ocdn",
            return_value=sa.text("INSERT"),
        ),
    ):
        await async_pd.copyinsert_dataframe(df, "t", "t_tmp", async_conn, schema="s")

    assert async_conn.execute.await_count == 3


@pytest.mark.asyncio
async def test_async_copyinsert_polars_delegates() -> None:
    from pgcopyinsert.asynchronous import pl as async_pl

    df = pl.DataFrame({"z": [9]})
    async_conn = MagicMock()

    with (
        patch.object(df, "write_csv") as mock_wcsv,
        patch(
            "pgcopyinsert.asynchronous.pl._copyinsert.copyinsert_csv",
            new_callable=AsyncMock,
        ) as mock_ci,
    ):
        await async_pl.copyinsert_polars(df, "items", "items_tmp", async_conn, sep="\t")

    mock_wcsv.assert_called_once()
    assert mock_wcsv.call_args.kwargs["separator"] == "\t"
    mock_ci.assert_awaited_once()
    pos, kw = mock_ci.await_args
    assert pos[1] == "items"
    assert kw["headers"] is False
    assert kw["columns"] == ["z"]


@pytest.mark.asyncio
async def test_async_pandas_dataframe_forwards_constraint_and_null() -> None:
    from pgcopyinsert.asynchronous import pd as async_pd

    df = pd.DataFrame({"a": [1]})
    async_conn = MagicMock()
    with (
        patch("pgcopyinsert.asynchronous.pd._write.write_df_bytes_csv"),
        patch(
            "pgcopyinsert.asynchronous.pd._copyinsert.copyinsert_csv",
            new_callable=AsyncMock,
        ) as mock_ci,
    ):
        await async_pd.copyinsert_dataframe(
            df,
            "t",
            "tmp",
            async_conn,
            constraint="t_pkey",
            null="\\N",
        )
    assert mock_ci.await_args.kwargs["constraint"] == "t_pkey"
    assert mock_ci.await_args.kwargs["null"] == "\\N"


@pytest.mark.asyncio
async def test_async_polars_forwards_constraint_to_copyinsert() -> None:
    from pgcopyinsert.asynchronous import pl as async_pl

    df = pl.DataFrame({"z": [1]})
    async_conn = MagicMock()
    with (
        patch.object(df, "write_csv"),
        patch(
            "pgcopyinsert.asynchronous.pl._copyinsert.copyinsert_csv",
            new_callable=AsyncMock,
        ) as mock_ci,
    ):
        await async_pl.copyinsert_polars(df, "p", "p_tmp", async_conn, constraint="p_z_key")
    assert mock_ci.await_args.kwargs["constraint"] == "p_z_key"
