import io
import pytest
from unittest.mock import Mock, patch

from pgcopyinsert.copy import copy_from_csv


@pytest.fixture
def csv_data() -> io.StringIO:
    return io.StringIO("""header1,header2\nvalue1,value2""")


def test_copy_from_csv_with_headers(csv_data) -> None:
    table_name = "test_table"
    with patch('sqlalchemy.engine.interfaces.DBAPICursor', autospec=True) as mock_dbapi_cursor:
        mock_dbapi_cursor.copy_from = Mock()
        copy_from_csv(mock_dbapi_cursor, csv_data, table_name, headers=True)
        mock_dbapi_cursor.copy_from.assert_called_once_with(
            csv_data,
            table_name,
            sep=',',
            null='',
            columns=['header1', 'header2']
        )


def test_copy_from_csv_without_headers(csv_data) -> None:
    table_name = "test_table"
    with patch('sqlalchemy.engine.interfaces.DBAPICursor', autospec=True) as mock_dbapi_cursor:
        mock_dbapi_cursor.copy_from = Mock()
        csv_data.seek(0)  # Resetting file pointer
        copy_from_csv(mock_dbapi_cursor, csv_data, table_name, headers=False, columns=['col1', 'col2'])
        mock_dbapi_cursor.copy_from.assert_called_once_with(
            csv_data,
            table_name,
            sep=',',
            null='',
            columns=['col1', 'col2']
        )


def test_copy_from_csv_with_schema(csv_data) -> None:
    table_name = "test_table"
    schema = "test_schema"
    with patch('sqlalchemy.engine.interfaces.DBAPICursor', autospec=True) as mock_dbapi_cursor:
        mock_dbapi_cursor.copy_from = Mock()
        copy_from_csv(mock_dbapi_cursor, csv_data, table_name, schema=schema)
        mock_dbapi_cursor.copy_from.assert_called_once_with(
            csv_data,
            f'{schema}.{table_name}',
            sep=',',
            null='',
            columns=['header1', 'header2']
        )
