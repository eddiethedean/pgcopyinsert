import typing as _t
import io as _io

import sqlalchemy as _sa
import fullmetalcopy.synchronous.copy as _copy

import pgcopyinsert.insert as _insert
import pgcopyinsert.temp as _temp


def copyinsert_csv(
    csv_file: _io.BytesIO,
    table_name: str,
    temp_name: str,
    connection: _sa.engine.base.Connection,
    sep=',',
    null='',
    columns=None,
    headers=True,
    schema=None,
    insert_function: _insert.InsertFunction = _insert.insert_from_table_stmt_ocdn,
    constraint: _t.Optional[str] = None
) -> None:
    meta = _sa.MetaData()
    meta.reflect(connection, schema=schema)
    target_table = _sa.Table(table_name, meta, schema=schema)
    # create temp table sqlalchemy object
    temp_table: _sa.Table = _temp.create_temp_table_from_table(target_table, temp_name, meta, columns=columns)

    # Create temp table
    create_stmt: _sa.sql.ddl.CreateTable = _temp.create_table_stmt(temp_table)
    connection.execute(create_stmt)

    # Copy csv to temp table
    _copy.copy_from_csv(
        connection, csv_file, temp_name,
        sep=sep, null=null, columns=columns,
        headers=headers
    )

    # Insert all records from temp table to target table
    stmt: _sa.Insert = insert_function(temp_table, target_table, constraint)
    connection.execute(stmt)

    # Drop temp table
    drop_table_stmt = _sa.schema.DropTable(temp_table, if_exists=True)
    connection.execute(drop_table_stmt)