import typing as _t

import sqlalchemy as _sa


def get_driver_connection(
    connection: _sa.engine.base.Connection
) -> 'psycopg2.extensions.connection | psycopg.Connection':
    connection_fairy = connection.connection
    return connection_fairy.driver_connection


def driver_copy_from_csv(
    driver: str
) -> _t.Callable:
    if driver == 'psycopg2':
        from pgcopyinsert.synchronous.pg2.copy import copy_from_csv
    elif driver == 'psycopg':
        from pgcopyinsert.synchronous.pg3.copy import copy_from_csv
    else:
        raise ValueError('connection string driver must be psycopg2 or psycopg')
    return copy_from_csv