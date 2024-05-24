import io as _io

import pyarrow as _pa
import pyarrow.csv as _pa_csv
import pandas as pd


def write_df_bytes_csv(
    df: pd.DataFrame,
    csv_file: _io.BytesIO,
    index: bool,
    include_headers: bool
) -> None:
    pa_df = _pa.Table.from_pandas(df, preserve_index=index)
    write_options = _pa_csv.WriteOptions(include_header=include_headers)
    _pa_csv.write_csv(pa_df, csv_file, write_options=write_options)