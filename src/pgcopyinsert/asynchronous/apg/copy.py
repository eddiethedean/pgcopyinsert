import typing as _t
import io as _io

import asyncpg as _asyncpg


async def copy_from_csv(
    async_connection: _asyncpg.Connection,
    csv_file: _io.TextIOWrapper,
    table_name: str,
    sep: str = ',',
    null: str = '',
    columns: _t.Optional[list[str]] = None,
    headers: bool = True,
    schema:_t.Optional[str] = None
) -> None:
    await async_connection.copy_to_table(
            table_name, source=csv_file, delimiter=sep,
            header=headers, null=null, columns=columns,
            schema_name=schema
        )