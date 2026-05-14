# pgcopyinsert: faster PostgreSQL bulk inserts

## What is it?

**pgcopyinsert** is a small Python library for faster PostgreSQL bulk loads: stream CSV (or Pandas/Polars) into a **temporary table** (same column names and types as the target, no constraints), then **`INSERT … SELECT`** into the real table, with optional **`ON CONFLICT DO NOTHING`** or **`DO UPDATE`**.

## Features

- **COPY** into a temp table, then insert into the target table (or upsert).
- **Synchronous** and **asynchronous** flows (via SQLAlchemy + [fullmetalcopy](https://pypi.org/project/fullmetalcopy/)).
- Helpers for **Pandas** and **Polars** DataFrames.
- Works with **psycopg** or **psycopg2** (sync) and **psycopg** or **asyncpg** (async), through SQLAlchemy URLs.

## Install

```sh
pip install pgcopyinsert
```

Core runtime dependencies are **SQLAlchemy** and **fullmetalcopy** (installed automatically).

Install a PostgreSQL driver and optional dataframe stack:

```sh
pip install pgcopyinsert[psycopg2]
pip install pgcopyinsert[psycopg]
pip install pgcopyinsert[asyncpg]
pip install pgcopyinsert[psycopg,pandas]
pip install pgcopyinsert[asyncpg,polars]
```

## Imports and modules

The package exports **`copy_from_csv`** and **`copyinsert_csv`** from the top level. Submodules hold temp-table helpers, insert statement builders, and dataframe entrypoints:

| Module | Role |
|--------|------|
| `pgcopyinsert` | `copy_from_csv`, `copyinsert_csv`, `__version__` |
| `pgcopyinsert.temp` | `create_temp_table_from_table`, `create_table_stmt` |
| `pgcopyinsert.insert` | `insert_from_table_stmt`, `insert_from_table_stmt_ocdn`, `insert_from_table_stmt_ocdu` |
| `pgcopyinsert.pd` | Pandas `copyinsert_dataframe` |
| `pgcopyinsert.pl` | Polars `copyinsert_polars` (`copyinsert_dataframe` is deprecated) |
| `pgcopyinsert.asynchronous.copyinsert` | async `copyinsert_csv` |
| `pgcopyinsert.asynchronous.pd` / `pl` | async dataframe helpers |

## `ON CONFLICT` and constraint names

For **`on_conflict_do_nothing`** / **`on_conflict_do_update`**, PostgreSQL expects a **named constraint** (e.g. `mytable_pkey`) or you rely on inference when no name is passed. The **`constraint=`** argument is the **constraint name in the database**, not always the same as a column name. Use `\d your_table` in `psql` or inspect `pg_constraint` to get the exact name.

## Examples (SQLAlchemy 2, sync)

```python
import io

import sqlalchemy as sa

from pgcopyinsert import copy_from_csv, copyinsert_csv
from pgcopyinsert.insert import (
    insert_from_table_stmt_ocdn,
    insert_from_table_stmt_ocdu,
)
from pgcopyinsert.temp import create_temp_table_from_table

engine = sa.create_engine("postgresql+psycopg2://scott:tiger@hostname/dbname")

# COPY bytes into an existing table (via fullmetalcopy)
with engine.connect() as conn:
    with open("data.csv", "rb") as f:
        buf = io.BytesIO(f.read())
    buf.seek(0)
    copy_from_csv(conn, buf, "staging_table", schema="public", headers=True)
    conn.commit()

# Reflect a table and build a TEMP table with the same columns (no constraints)
meta = sa.MetaData()
meta.reflect(engine, schema="public")
table = sa.Table("target_table", meta, schema="public")
other_meta = sa.MetaData()
temp_table = create_temp_table_from_table(table, "target_load_tmp", other_meta)

# copyinsert: temp DDL → COPY → INSERT from temp → DROP temp
with engine.connect() as conn:
    with open("data.csv", "rb") as f:
        buf = io.BytesIO(f.read())
    buf.seek(0)
    copyinsert_csv(
        buf,
        "target_table",
        "target_load_tmp",
        conn,
        schema="public",
        insert_function=insert_from_table_stmt_ocdn,
        constraint="target_table_pkey",  # real constraint name in DB
    )
    conn.commit()

# Upsert (DO UPDATE) — still use the DB constraint name
with engine.connect() as conn:
    with open("data.csv", "rb") as f:
        buf = io.BytesIO(f.read())
    buf.seek(0)
    copyinsert_csv(
        buf,
        "target_table",
        "target_load_tmp",
        conn,
        schema="public",
        insert_function=insert_from_table_stmt_ocdu,
        constraint="target_table_pkey",
    )
    conn.commit()
```

Using **`engine.begin()`** is recommended when you want create/copy/insert/drop in a **single transaction**.

### Pandas

```python
import pandas as pd
import sqlalchemy as sa

from pgcopyinsert.insert import insert_from_table_stmt_ocdu
from pgcopyinsert import pd as pci_pd

engine = sa.create_engine("postgresql+psycopg2://...")
df = pd.DataFrame({"x": range(1000), "y": range(1000)})
with engine.connect() as conn:
    pci_pd.copyinsert_dataframe(
        df,
        "xy_table",
        "xy_table_tmp",
        conn,
        insert_function=insert_from_table_stmt_ocdu,
        constraint="xy_table_pkey",
    )
    conn.commit()
```

### Polars (sync)

Use **`copyinsert_polars`**. The name **`copyinsert_dataframe`** in this module is deprecated.

```python
import polars as pl
import sqlalchemy as sa

from pgcopyinsert.insert import insert_from_table_stmt_ocdu
from pgcopyinsert import pl as pci_pl

engine = sa.create_engine("postgresql+psycopg2://...")
df = pl.DataFrame({"x": range(1000), "y": range(1000)})
with engine.connect() as conn:
    pci_pl.copyinsert_polars(
        df,
        "xy_table",
        "xy_table_tmp",
        conn,
        insert_function=insert_from_table_stmt_ocdu,
        constraint="xy_table_pkey",
    )
    conn.commit()
```

### Async (sketch)

```python
import io

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from pgcopyinsert.asynchronous.copyinsert import copyinsert_csv

async_engine = create_async_engine("postgresql+asyncpg://...")
async with async_engine.connect() as conn:
    buf = io.BytesIO(b"id,name\n1,ada\n")
    await copyinsert_csv(buf, "people", "people_tmp", conn, schema="public")
    await conn.commit()
```

Use **`pgcopyinsert.asynchronous.pd`** / **`pgcopyinsert.asynchronous.pl`** for async DataFrame loads.

## Source

https://github.com/eddiethedean/pgcopyinsert
