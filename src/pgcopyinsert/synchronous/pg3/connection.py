import psycopg as _psycopg
import sqlalchemy as _sa


def get_driver_connection(
    connection: _sa.engine.base.Connection
) -> _psycopg.Connection:
    connection_fairy: _sa.PoolProxiedConnection = connection.connection
    return connection_fairy.driver_connection