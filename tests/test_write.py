import io

import pandas as pd

from pgcopyinsert.write import write_df_bytes_csv


def test_write_df_bytes_csv_with_header_contains_column_names() -> None:
    buf = io.BytesIO()
    df = pd.DataFrame({"alpha": [1, 2], "beta": ["x", "y"]})
    write_df_bytes_csv(df, buf, index=False, include_headers=True)
    raw = buf.getvalue().decode()
    assert "alpha" in raw and "beta" in raw
    lines = [ln for ln in raw.strip().splitlines() if ln]
    assert lines[0].startswith("alpha") or "alpha" in lines[0]


def test_write_df_bytes_csv_without_header_omits_header_row() -> None:
    buf = io.BytesIO()
    df = pd.DataFrame({"n": [42]})
    write_df_bytes_csv(df, buf, index=False, include_headers=False)
    raw = buf.getvalue().decode()
    assert "n" not in raw.splitlines()[0] if raw else True
    assert "42" in raw
