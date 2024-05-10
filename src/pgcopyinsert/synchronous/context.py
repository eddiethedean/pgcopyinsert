import contextlib as _contextlib


@_contextlib.contextmanager
def get_cursor(
    connection: 'psycopg2.Connection | psycopg.Connection'
) -> 'psycopg2.Cursor | psycopg.Cursor':
    cursor = connection.cursor()
    try:
        yield cursor
    finally:
        cursor.close()