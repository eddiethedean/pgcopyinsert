import typing as _t

import pandas as _pd
import sqlalchemy.ext.asyncio as _sa_asyncio

import pgcopyinsert.insert as _insert
import pgcopyinsert.asynchronous.copyinsert as _copyinsert
import pgcopyinsert.asynchronous.drivers as _async_drivers
import pgcopyinsert.drivers as _drivers


async def copyinsert_dataframe(
    df: _pd.DataFrame,
    table_name: str,
    temp_name: str,
    async_connection: _sa_asyncio.AsyncConnection,
    index: bool = False,
    sep: str = ',',
    encoding: str = 'utf8',
    schema: _t.Optional[str] = None,
    insert_function: _insert.InsertFunction = _insert.insert_from_table_stmt_ocdn
) -> None:
    driver: str = _drivers.connection_driver_name(async_connection)
    DriverIO = _async_drivers.get_driver_io(driver)
    with DriverIO() as csv_file:
        df.to_csv(csv_file, sep=sep, header=False, encoding=encoding, index=index)
        csv_file.seek(0)
        column_names = list(df.columns)
        await _copyinsert.copyinsert_csv(
            csv_file, table_name, temp_name, async_connection,
            sep=sep, headers=False, schema=schema,
            columns=column_names, insert_function=insert_function
        )