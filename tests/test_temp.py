import unittest

import pytest
import sqlalchemy as sa

from pgcopyinsert.temp import create_temp_table_from_table
from pgcopyinsert.temp import create_table_stmt

@pytest.fixture
def test_tables() -> tuple[sa.Table, sa.Table]:
    metadata = sa.MetaData()
    test_table = sa.Table('test_table', metadata, sa.Column('id', sa.Integer, primary_key=True))
    temp_test_table = sa.Table('temp_test_table', metadata, sa.Column('id', sa.Integer))
    return test_table, temp_test_table


def test_create_temp_table_from_table(test_tables):
    test_table, temp_table= test_tables
    metadata = sa.MetaData()
    result: sa.Table = create_temp_table_from_table(test_table, 'temp_test_table', metadata)
    assert result.compare(temp_table)

def test_create_table_stmt(test_tables):
    ...