import typing as _t
from io import BytesIO, StringIO

import pandas as pd
import sqlalchemy as sa
import polars as pl
from sqlalchemy.ext.asyncio import AsyncEngine
import aiofiles
import aiocsv
from aiofiles.threadpool.text import AsyncTextIOWrapper

from pgcopyinsert.context import asyncpg_connection
from pgcopyinsert.insert import insert_from_table_stmt
from pgcopyinsert.temp import create_table_stmt, create_temp_table_from_table


async def copyinsert_csv(
    async_csv_file,
    table_name: str,
    temp_name: str,
    async_engine: AsyncEngine,
    sep=',',
    null='',
    columns=None,
    headers=True,
    schema=None,
    insert_function: _t.Callable[[sa.Table, sa.Table], sa.Insert] = insert_from_table_stmt
):
    meta = sa.MetaData()
    async with async_engine.begin() as conn:
        await conn.run_sync(meta.reflect, schema=schema)
    target_table = sa.Table(table_name, meta, schema=schema)
    # create temp table sqlalchemy object
    temp_table: sa.Table = create_temp_table_from_table(target_table, temp_name, meta, columns=columns)

    async with async_engine.connect() as async_conn:
         # Create temp table
        create_stmt: sa.sql.ddl.CreateTable = create_table_stmt(temp_table)
        await async_conn.execute(create_stmt)
    
        async with asyncpg_connection(async_conn) as asyncpg_conn:
            # Copy csv to temp table
            await asyncpg_conn.copy_to_table(
                temp_name, source=csv_file, delimiter=sep,
                header=headers, null=null, schema_name=schema
            )

        # Insert all records from temp table to target table
        insert_stmt: sa.Insert = insert_function(temp_table, target_table)
        await async_conn.execute(insert_stmt)

        await async_conn.commit()
    # Drop temp table
    async with async_engine.begin() as conn:
        await conn.run_sync(temp_table.drop)


async def copyinsert_dataframe(
    df: pd.DataFrame,
    table_name: str,
    temp_name: str,
    async_engine: AsyncEngine,
    index=False,
    sep=',',
    encoding='utf8',
    schema=None,
    insert_function: _t.Callable[[sa.Table, sa.Table], sa.Insert] = insert_from_table_stmt
) -> None:
    with StringIO() as csv_file:
        # write DataFrame to in memory csv
        df.to_csv(csv_file, sep=sep, header=False, encoding=encoding, index=index)
        csv_file.seek(0)
        column_names = list(df.columns)
        await copyinsert_csv(csv_file, table_name, temp_name, async_engine, sep, encoding,
                       headers=False, schema=schema, columns=column_names,
                       insert_function=insert_function)


async def copyinsert_polars(
    df: pl.DataFrame,
    table_name: str,
    temp_name: str,
    async_engine: AsyncEngine,
    sep=',',
    encoding='utf8',
    schema=None,
    insert_function: _t.Callable[[sa.Table, sa.Table], sa.Insert] = insert_from_table_stmt
) -> None:
    with BytesIO() as csv_file:
        # write DataFrame to in memory csv
        df.write_csv(csv_file, include_header=False, null_value='', separator=sep)
        csv_file.seek(0)
        column_names = list(df.columns)
        await copyinsert_csv(csv_file, table_name, temp_name, async_engine, sep, encoding,
                       headers=False, schema=schema, columns=column_names,
                       insert_function=insert_function)