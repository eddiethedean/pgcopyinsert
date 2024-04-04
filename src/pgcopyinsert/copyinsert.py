import typing as _t
from io import BytesIO, StringIO

import pandas as pd
import sqlalchemy as sa
import polars as pl

from pgcopyinsert.context import raw_connection, raw_cursor
from pgcopyinsert.copy import copy_from_csv
from pgcopyinsert.insert import insert_from_table_stmt
from pgcopyinsert.temp import create_table_stmt, create_temp_table_from_table


def copyinsert_csv(
    csv_file,
    table_name: str,
    temp_name: str,
    engine: sa.engine.Engine,
    sep=',',
    null='',
    columns=None,
    headers=True,
    schema=None,
    insert_function: _t.Callable[[sa.Table, sa.Table], sa.Insert] = insert_from_table_stmt
):
    meta = sa.MetaData()
    meta.reflect(engine, schema=schema)
    target_table = sa.Table(table_name, meta, schema=schema)
    # create temp table sqlalchemy object
    temp_table: sa.Table = create_temp_table_from_table(target_table, temp_name, meta, columns=columns)
    
    with raw_connection(engine) as connection:
        with raw_cursor(connection) as cursor:
            # Create temp table
            create_stmt: sa.sql.ddl.CreateTable = create_table_stmt(temp_table)
            cursor.execute(str(create_stmt))
            
            # Copy csv to temp table
            copy_from_csv(cursor, csv_file, temp_name, sep, null, columns, headers=headers)

            # Insert all records from temp table to target table
            stmt: sa.Insert = insert_function(temp_table, target_table)
            cursor.execute(str(stmt))

            connection.commit()
    # Drop temp table
    temp_table.drop(engine)


def copyinsert_dataframe(
    df: pd.DataFrame,
    table_name: str,
    temp_name: str,
    engine: sa.engine.Engine,
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
        copyinsert_csv(csv_file, table_name, temp_name, engine, sep, encoding,
                       headers=False, schema=schema, columns=column_names,
                       insert_function=insert_function)


def copyinsert_polars(
    df: pl.DataFrame,
    table_name: str,
    temp_name: str,
    engine: sa.engine.Engine,
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
        copyinsert_csv(csv_file, table_name, temp_name, engine, sep, encoding,
                       headers=False, schema=schema, columns=column_names,
                       insert_function=insert_function)