from typing import List
import testing.postgresql
from sqlalchemy import create_engine
import sqlalchemy as sa



# Lanuch new PostgreSQL server
with testing.postgresql.Postgresql() as postgresql:
    # connect to PostgreSQL
    engine: sa.Engine = create_engine(postgresql.url())
    inspector: sa.Inspector = sa.inspect(engine)
    table_names: List[str] = inspector.get_table_names()

    print(table_names)
    # if you use postgresql or other drivers:
    #   import psycopg2
    #   db = psycopg2.connect(**postgresql.dsn())

    #
    # do any tests using PostgreSQL...
    #

# PostgreSQL server is terminated here