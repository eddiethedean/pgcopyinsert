import io as _io

import pandas as _pd
import sqlalchemy.ext.asyncio as _sa_asyncio

import pgcopyinsert.asynchronous.copyinsert as _copyinsert
import pgcopyinsert.insert as _insert
import pgcopyinsert.write as _write


async def copyinsert_dataframe(
    df: _pd.DataFrame,
    table_name: str,
    temp_name: str,
    async_connection: _sa_asyncio.AsyncConnection,
    index: bool = False,
    sep: str = ",",
    schema: str | None = None,
    insert_function: _insert.InsertFunction = _insert.insert_from_table_stmt_ocdn,
    constraint: str | None = None,
    null: str = "",
) -> None:
    with _io.BytesIO() as csv_file:
        _write.write_df_bytes_csv(df, csv_file, index, include_headers=True)
        csv_file.seek(0)
        column_names = list(df.columns)
        await _copyinsert.copyinsert_csv(
            csv_file,
            table_name,
            temp_name,
            async_connection,
            sep=sep,
            null=null,
            headers=True,
            schema=schema,
            columns=column_names,
            insert_function=insert_function,
            constraint=constraint,
        )
