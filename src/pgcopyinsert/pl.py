import io as _io
import warnings

import polars as _pl
import sqlalchemy as _sa

import pgcopyinsert.copyinsert as _copyinsert
import pgcopyinsert.insert as _insert


def copyinsert_polars(
    df: _pl.DataFrame,
    table_name: str,
    temp_name: str,
    connection: _sa.engine.base.Connection,
    sep=",",
    schema=None,
    insert_function: _insert.InsertFunction = _insert.insert_from_table_stmt_ocdn,
    constraint: str | None = None,
    null: str = "",
) -> None:
    with _io.BytesIO() as csv_file:
        df.write_csv(csv_file, include_header=False, null_value="", separator=sep)
        csv_file.seek(0)
        column_names = list(df.columns)
        _copyinsert.copyinsert_csv(
            csv_file,
            table_name,
            temp_name,
            connection,
            sep=sep,
            null=null,
            headers=False,
            schema=schema,
            columns=column_names,
            insert_function=insert_function,
            constraint=constraint,
        )


def copyinsert_dataframe(
    df: _pl.DataFrame,
    table_name: str,
    temp_name: str,
    connection: _sa.engine.base.Connection,
    sep=",",
    schema=None,
    insert_function: _insert.InsertFunction = _insert.insert_from_table_stmt_ocdn,
    constraint: str | None = None,
    null: str = "",
) -> None:
    warnings.warn(
        "copyinsert_dataframe for Polars is deprecated; use copyinsert_polars instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    copyinsert_polars(
        df,
        table_name,
        temp_name,
        connection,
        sep=sep,
        schema=schema,
        insert_function=insert_function,
        constraint=constraint,
        null=null,
    )
