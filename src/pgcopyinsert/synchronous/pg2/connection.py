import psycopg2 as _psycopg2
import sqlalchemy as _sa


def get_driver_connection(
    connection: _sa.engine.base.Connection
) -> _psycopg2.extensions.connection:
    connection_fairy: _sa.PoolProxiedConnection = connection.connection
    return connection_fairy.driver_connection



