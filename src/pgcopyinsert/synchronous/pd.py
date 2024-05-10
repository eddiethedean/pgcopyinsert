import io as _io

import pyarrow as _pa
import pyarrow.csv as _pa_csv
import pandas as _pd
import sqlalchemy as _sa

import pgcopyinsert.insert as _insert
import pgcopyinsert.synchronous.copyinsert as _copyinsert


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
        pa_df = _pa.Table.from_pandas(df, preserve_index=index)
        write_options = _pa_csv.WriteOptions(include_header=True)
        _pa_csv.write_csv(pa_df, csv_file, write_options=write_options)
        csv_file.seek(0)
        column_names = list(df.columns)
        _copyinsert.copyinsert_csv(
            csv_file, table_name, temp_name, connection,
            sep=sep, null='', headers=True, schema=schema,
            columns=column_names, insert_function=insert_function
        )