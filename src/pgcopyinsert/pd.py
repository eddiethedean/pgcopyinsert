import io as _io

import pandas as _pd
import sqlalchemy as _sa

import pgcopyinsert.insert as _insert
import pgcopyinsert.copyinsert as _copyinsert
import pgcopyinsert.write as _write


def copyinsert_dataframe(
    df: _pd.DataFrame,
    table_name: str,
    temp_name: str,
    connection: _sa.engine.base.Connection,
    index=False,
    sep=',',
    schema=None,
    insert_function: _insert.InsertFunction = _insert.insert_from_table_stmt_ocdn
) -> None:
    with _io.BytesIO() as csv_file:
        _write.write_df_bytes_csv(df, csv_file, index, include_headers=True)
        csv_file.seek(0)
        column_names = list(df.columns)
        _copyinsert.copyinsert_csv(
            csv_file, table_name, temp_name, connection,
            sep=sep, null='', headers=True, schema=schema,
            columns=column_names, insert_function=insert_function
        )