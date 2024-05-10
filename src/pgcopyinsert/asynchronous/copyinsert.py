import typing as _t

import sqlalchemy as _sa
import sqlalchemy.ext.asyncio as _sa_asyncio

import pgcopyinsert.insert as _insert
import pgcopyinsert.temp as _temp
import pgcopyinsert.asynchronous.copy as _copy


async def copyinsert_csv(
    csv_file,
    table_name: str,
    temp_name: str,
    async_connection: _sa_asyncio.AsyncConnection,
    sep=',',
    null='',
    columns=None,
    headers=True,
    schema=None,
    insert_function: _insert.InsertFunction = _insert.insert_from_table_stmt_ocdn,
    constraint: _t.Optional[str] = None
) -> None:
    meta = _sa.MetaData()
    await async_connection.run_sync(meta.reflect, schema=schema)
    target_table = _sa.Table(table_name, meta, schema=schema)
    # create temp table sqlalchemy object
    temp_table: _sa.Table = _temp.create_temp_table_from_table(target_table, temp_name, meta, columns=columns)
    # Create temp table
    create_stmt: _sa.sql.ddl.CreateTable = _temp.create_table_stmt(temp_table)
    await async_connection.execute(create_stmt)

    # Copy csv to temp table
    await _copy.copy_from_csv(
        async_connection, csv_file, temp_table.name,
        sep=sep, null=null, columns=columns,
        headers=headers, schema=None
    )

    # Insert all records from temp table to target table
    stmt: _sa.Insert = insert_function(temp_table, target_table, constraint)
    await async_connection.execute(stmt)

    # Drop temp table
    drop_table_stmt = _sa.schema.DropTable(temp_table, if_exists=True)
    await async_connection.execute(drop_table_stmt)