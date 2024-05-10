import sqlalchemy.ext.asyncio as _sa_asyncio
import sqlalchemy as _sa


def connection_driver_name(
    connection: _sa.engine.base.Connection | _sa_asyncio.AsyncConnection
) -> str:
    return connection.dialect.driver