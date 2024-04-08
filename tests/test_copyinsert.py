from unittest.mock import MagicMock, Mock, patch
import io

import pytest
import pandas as pd
import polars as pl
import sqlalchemy as sa

from pgcopyinsert.copyinsert import copyinsert_csv
from pgcopyinsert.copyinsert import copyinsert_dataframe
from pgcopyinsert.copyinsert import copyinsert_polars


@pytest.fixture
def csv_data() -> io.StringIO:
    return io.StringIO("""header1,header2\nvalue1,value2""")


@pytest.fixture
def engine() -> MagicMock:
    return MagicMock(spec=sa.engine.Engine)


def test_copyinsert_csv(csv_data, engine) -> None:
    ...

def test_copyinsert_dataframe():
    ...


def test_copyinsert_polars():
    ...