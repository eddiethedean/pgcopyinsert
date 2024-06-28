import typing as _t
import io as _io

import polars as _pl
import sqlalchemy.ext.asyncio as _sa_asyncio

import pgcopyinsert.insert as _insert
import pgcopyinsert.asynchronous.copyinsert as _copyinsert


async def copyinsert_polars(
    df: _pl.DataFrame,
    table_name: str,
    temp_name: str,
    async_connection: _sa_asyncio.AsyncConnection,
    sep: str = ',',
    schema: _t.Optional[str] = None,
    insert_function: _insert.InsertFunction = _insert.insert_from_table_stmt_ocdn
) -> None:
    with _io.BytesIO() as csv_file:
        df.write_csv(csv_file, include_header=False, null_value='', separator=sep)
        csv_file.seek(0)
        column_names = list(df.columns)
        await _copyinsert.copyinsert_csv(
            csv_file, table_name, temp_name, async_connection,
            sep=sep, headers=False, schema=schema,
            columns=column_names, insert_function=insert_function
        )