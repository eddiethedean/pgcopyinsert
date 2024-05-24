import psycopg.sql as _sql


def create_copy_query(table_name, fields) -> _sql.Composed:
    return _sql.SQL("COPY {table} ({fields}) FROM STDOUT").format(
        fields=_sql.SQL(',').join([_sql.Identifier(col) for col in fields]),
        table=_sql.Identifier(table_name)
    )