import io
from typing import Any, Generator, Sequence
import pytest
from unittest.mock import Mock, patch

from sqlalchemy.engine.interfaces import DBAPICursor
from sqlalchemy.engine.row import Row
import psycopg2

from pgcopyinsert.copy import copy_from_csv
import testing.postgresql
from sqlalchemy import CursorResult, Engine, PoolProxiedConnection, create_engine, MetaData, Table

from models import Base


@pytest.fixture
def csv_data_headers() -> io.StringIO:
    return io.StringIO("""first_name,last_name\nOdos,Matthews\nJohn,Doe""")


@pytest.fixture
def csv_data_no_headers() -> io.StringIO:
    return io.StringIO("""Odos,Matthews\nJohn,Doe""")


@pytest.fixture
def csv_data_too_many_cols() -> io.StringIO:
    return io.StringIO("""first_name,last_name,age\nOdos,Matthews,38\nJohn,Doe,35""")


@pytest.fixture
def csv_data_headers_dif_order() -> io.StringIO:
    return io.StringIO("""last_name,first_name\nMatthews,Odos\nDoe,John""")


@pytest.fixture(scope='session', autouse=True)
def db_factory() -> Generator[testing.postgresql.PostgresqlFactory, None, None]:
    # create initial data on create as fixtures into the database
    def handler(postgresql) -> None:
        engine = create_engine(postgresql.url())
        Base.metadata.create_all(engine)
    # Will be executed before the first test
    Postgresql = testing.postgresql.PostgresqlFactory(cache_initialized_db=True,
                                                  on_initialized=handler)
    yield Postgresql
    # Will be executed after the last test
    Postgresql.clear_cache()


@pytest.fixture
def setup_database(db_factory) -> Generator[testing.postgresql.Postgresql, None, None]:
    postgresql: testing.postgresql.Postgresql = db_factory()
    yield postgresql
    postgresql.stop()


def test_copy_from_csv_with_headers_real(csv_data_headers, setup_database) -> None:
    engine = create_engine(setup_database.url())
    raw_connection: PoolProxiedConnection = engine.raw_connection()
    cursor: DBAPICursor = raw_connection.cursor()
    copy_from_csv(cursor, csv_data_headers, 'users', headers=True)
    raw_connection.commit()
    raw_connection.close()
    metadata = MetaData()
    metadata.reflect(engine)
    table = Table('users', metadata)
    with engine.connect() as connection:
        result: CursorResult[Any] = connection.execute(table.select())
        records: Sequence[Row[Any]] = result.fetchall()
    engine.dispose()
    assert records == [(1, 'Odos', 'Matthews'),
                       (2, 'John', 'Doe')]
    

def test_copy_from_csv_with_headers__diforder_real(csv_data_headers_dif_order, setup_database) -> None:
    engine = create_engine(setup_database.url())
    raw_connection: PoolProxiedConnection = engine.raw_connection()
    cursor: DBAPICursor = raw_connection.cursor()
    copy_from_csv(cursor, csv_data_headers_dif_order, 'users', headers=True)
    raw_connection.commit()
    raw_connection.close()
    metadata = MetaData()
    metadata.reflect(engine)
    table = Table('users', metadata)
    with engine.connect() as connection:
        result: CursorResult[Any] = connection.execute(table.select())
        records: Sequence[Row[Any]] = result.fetchall()
    engine.dispose()
    assert records == [(1, 'Odos', 'Matthews'),
                       (2, 'John', 'Doe')]
    

def test_copy_from_csv_no_headers_real(csv_data_no_headers, setup_database) -> None:
    engine: Engine = create_engine(setup_database.url())
    raw_connection: PoolProxiedConnection = engine.raw_connection()
    cursor: DBAPICursor = raw_connection.cursor()
    copy_from_csv(cursor, csv_data_no_headers, 'users', headers=False, columns=['first_name', 'last_name'])
    raw_connection.commit()
    raw_connection.close()
    metadata = MetaData()
    metadata.reflect(engine)
    table = Table('users', metadata)
    with engine.connect() as connection:
        result: CursorResult[Any] = connection.execute(table.select())
        records: Sequence[Row[Any]] = result.fetchall()
    engine.dispose()
    assert records == [(1, 'Odos', 'Matthews'),
                       (2, 'John', 'Doe')]
    

def test_copy_from_csv_too_many_cols(csv_data_too_many_cols, setup_database) -> None:
    engine: Engine = create_engine(setup_database.url())
    raw_connection: PoolProxiedConnection = engine.raw_connection()
    cursor: DBAPICursor = raw_connection.cursor()
    with pytest.raises(psycopg2.errors.UndefinedColumn):
        try:
            copy_from_csv(cursor, csv_data_too_many_cols, 'users', headers=True)
        except Exception as e:
            raw_connection.close()
            raise e


def test_copy_from_csv_with_headers(csv_data_headers) -> None:
    table_name = "test_table"
    columns: list[str] = ['first_name', 'last_name']
    with patch('sqlalchemy.engine.interfaces.DBAPICursor', autospec=True) as mock_dbapi_cursor:
        mock_dbapi_cursor.copy_from = Mock()
        copy_from_csv(mock_dbapi_cursor, csv_data_headers, table_name, headers=True)
        mock_dbapi_cursor.copy_from.assert_called_once_with(
            csv_data_headers,
            table_name,
            sep=',',
            null='',
            columns=columns
        )


def test_copy_from_csv_without_headers(csv_data_no_headers) -> None:
    table_name = "test_table"
    columns: list[str] = ['first_name', 'last_name']
    with patch('sqlalchemy.engine.interfaces.DBAPICursor', autospec=True) as mock_dbapi_cursor:
        mock_dbapi_cursor.copy_from = Mock()
        csv_data_no_headers.seek(0)  # Resetting file pointer
        copy_from_csv(mock_dbapi_cursor, csv_data_no_headers, table_name, headers=False, columns=columns)
        mock_dbapi_cursor.copy_from.assert_called_once_with(
            csv_data_no_headers,
            table_name,
            columns=columns,
            sep=',',
            null=''
        )


def test_copy_from_csv_with_schema(csv_data_no_headers) -> None:
    table_name = "test_table"
    schema = "test_schema"
    columns: list[str] = ['first_name', 'last_name']
    with patch('sqlalchemy.engine.interfaces.DBAPICursor', autospec=True) as mock_dbapi_cursor:
        mock_dbapi_cursor.copy_from = Mock()
        copy_from_csv(mock_dbapi_cursor, csv_data_no_headers, table_name, schema=schema, headers=False, columns=columns)
        mock_dbapi_cursor.copy_from.assert_called_once_with(
            csv_data_no_headers,
            f'{schema}.{table_name}',
            sep=',',
            null='',
            columns=columns
        )
