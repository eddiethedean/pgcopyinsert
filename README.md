# pgcopyinsert

[![PyPI version](https://img.shields.io/pypi/v/pgcopyinsert)](https://pypi.org/project/pgcopyinsert/)
[![Python versions](https://img.shields.io/pypi/pyversions/pgcopyinsert)](https://pypi.org/project/pgcopyinsert/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Fast PostgreSQL bulk loads** using a temp table: **`COPY`** into `TEMPORARY …`, then **`INSERT … SELECT`** (optionally **`ON CONFLICT DO NOTHING`** / **`DO UPDATE`**). Built on **SQLAlchemy 2** and **[fullmetalcopy](https://pypi.org/project/fullmetalcopy/)** (sync + async `COPY`).

---

## Contents

- [Why this pattern](#why-this-pattern)
- [Requirements](#requirements)
- [Install](#install)
- [Quick start](#quick-start)
- [Package layout](#package-layout)
- [Main APIs](#main-apis)
- [`ON CONFLICT` and `constraint`](#on-conflict-and-constraint)
- [Examples](#examples)
  - [CSV sync](#csv-sync-sqlalchemy-2)
  - [Single transaction](#single-transaction-with-enginebegin)
  - [Pandas](#pandas)
  - [Polars](#polars-sync)
  - [Async](#async)
- [Development](#development)
- [Links](#links)

---

## Why this pattern

1. **Reflect** the target table so column names and types match PostgreSQL.
2. **Create** a `TEMPORARY` table with the same columns (no constraints).
3. **`COPY`** CSV bytes into the temp table (fast path).
4. **`INSERT … SELECT`** into the real table (your chosen insert / upsert builder).
5. **`DROP`** the temp table.

This keeps `COPY` simple and pushes deduplication / upsert rules into normal SQL `INSERT` logic.

```mermaid
flowchart LR
  reflect[Reflect_target]
  temp[Create_TEMP]
  copy[COPY_to_temp]
  ins[INSERT_from_temp]
  drop[DROP_temp]
  reflect --> temp --> copy --> ins --> drop
```

---

## Requirements

- **Python** 3.10+
- **PostgreSQL** (server your SQLAlchemy engine points at)
- Runtime deps: **SQLAlchemy ≥ 2.0**, **fullmetalcopy ≥ 0.2.0** (installed with `pip install pgcopyinsert`)
- A DB driver in your environment (**psycopg**, **psycopg2**, or **asyncpg**) matching your SQLAlchemy URL

---

## Install

```sh
pip install pgcopyinsert
```

**Extras** (drivers + optional stacks):

```sh
pip install pgcopyinsert[psycopg2]
pip install pgcopyinsert[psycopg]
pip install pgcopyinsert[asyncpg]
pip install pgcopyinsert[psycopg,pandas]
pip install pgcopyinsert[asyncpg,polars]
```

If `pip` cannot resolve **fullmetalcopy** right after a release, upgrade pip (`pip install -U pip`) and retry.

---

## Quick start

```python
import io

import sqlalchemy as sa

from pgcopyinsert import copyinsert_csv

engine = sa.create_engine("postgresql+psycopg2://user:pass@host/dbname")

with engine.begin() as conn:
    buf = io.BytesIO(b"id,name\n1,Ada\n")
    copyinsert_csv(buf, "people", "people_load_tmp", conn, schema="public", headers=True)
```

`copyinsert_csv` expects a **binary** CSV stream (`io.BytesIO` or file opened with `"rb"`). Use **`engine.begin()`** so temp DDL, copy, insert, and drop stay in **one transaction**.

---

## Package layout

| Import | What you get |
|--------|----------------|
| `from pgcopyinsert import copy_from_csv, copyinsert_csv, __version__` | Top-level CSV + copyinsert entrypoints |
| `pgcopyinsert.temp` | `create_temp_table_from_table`, `create_table_stmt` |
| `pgcopyinsert.insert` | `insert_from_table_stmt`, `insert_from_table_stmt_ocdn`, `insert_from_table_stmt_ocdu` |
| `pgcopyinsert.pd` | Pandas `copyinsert_dataframe` |
| `pgcopyinsert.pl` | Polars **`copyinsert_polars`** (`copyinsert_dataframe` is deprecated) |
| `pgcopyinsert.asynchronous.copyinsert` | `async copyinsert_csv` |
| `pgcopyinsert.asynchronous.pd` / `pl` | Async Pandas / Polars helpers |

---

## Main APIs

### `copyinsert_csv` (sync)

| Parameter | Description |
|-----------|-------------|
| `csv_file` | `BytesIO` (or compatible binary buffer), positioned at start of CSV |
| `table_name` | Existing target table name (reflected) |
| `temp_name` | Name for the `TEMPORARY` load table |
| `connection` | SQLAlchemy `Connection` |
| `sep`, `null`, `columns`, `headers`, `schema` | Forwarded to **fullmetalcopy** `copy_from_csv` for the temp table |
| `insert_function` | Callable `(temp_table, target_table, constraint) →` executable insert (default: DO NOTHING) |
| `constraint` | PostgreSQL **constraint name** for `ON CONFLICT`, when required by your insert builder |

### `copy_from_csv` (sync)

Thin re-export from **fullmetalcopy**: load CSV bytes **directly** into a normal table (no temp-table orchestration). See fullmetalcopy docs for parameters.

### DataFrame helpers

**Pandas** (`pgcopyinsert.pd.copyinsert_dataframe`) and **Polars** (`pgcopyinsert.pl.copyinsert_polars`) accept the same idea: optional `constraint`, `null`, `schema`, `sep`, and `insert_function`, and delegate to `copyinsert_csv` / async equivalents.

---

## `ON CONFLICT` and `constraint`

PostgreSQL’s `ON CONFLICT` often targets a **named constraint** (for example `mytable_pkey`). The `constraint=` string must match **`pg_constraint.conname`** (use `\d tablename` in `psql` or query the catalog). It is **not** always the same as a column name.

---

## Examples

### CSV sync (SQLAlchemy 2)

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

# COPY into an existing table (fullmetalcopy)
with engine.connect() as conn:
    with open("data.csv", "rb") as f:
        buf = io.BytesIO(f.read())
    buf.seek(0)
    copy_from_csv(conn, buf, "staging_table", schema="public", headers=True)
    conn.commit()

# Reflect + build a TEMP table with the same columns (no constraints)
meta = sa.MetaData()
meta.reflect(engine, schema="public")
table = sa.Table("target_table", meta, schema="public")
other_meta = sa.MetaData()
temp_table = create_temp_table_from_table(table, "target_load_tmp", other_meta)

# copyinsert: temp DDL → COPY → INSERT → DROP temp
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
        constraint="target_table_pkey",
    )
    conn.commit()

# Upsert (DO UPDATE)
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

### Single transaction with `engine.begin()`

```python
import io

import sqlalchemy as sa

from pgcopyinsert import copyinsert_csv

engine = sa.create_engine("postgresql+psycopg2://user:pass@host/db")

with engine.begin() as conn:
    buf = io.BytesIO(b"id\n42\n")
    copyinsert_csv(buf, "my_table", "my_table_tmp", conn, headers=True)
```

### Pandas

```python
import pandas as pd
import sqlalchemy as sa

from pgcopyinsert.insert import insert_from_table_stmt_ocdu
import pgcopyinsert.pd as pci_pd

engine = sa.create_engine("postgresql+psycopg2://user:pass@host/db")
df = pd.DataFrame({"x": range(1000), "y": range(1000)})

with engine.connect() as conn:
    pci_pd.copyinsert_dataframe(
        df,
        "xy_table",
        "xy_table_tmp",
        conn,
        insert_function=insert_from_table_stmt_ocdu,
        constraint="xy_table_pkey",
        null="",
    )
    conn.commit()
```

### Polars (sync)

Prefer **`copyinsert_polars`**. **`copyinsert_dataframe`** in this module is deprecated and will emit `DeprecationWarning`.

```python
import polars as pl
import sqlalchemy as sa

from pgcopyinsert.insert import insert_from_table_stmt_ocdu
import pgcopyinsert.pl as pci_pl

engine = sa.create_engine("postgresql+psycopg2://user:pass@host/db")
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

### Async

```python
import io

from sqlalchemy.ext.asyncio import create_async_engine

from pgcopyinsert.asynchronous.copyinsert import copyinsert_csv

async def load_once(dsn: str) -> None:
    engine = create_async_engine(dsn)
    async with engine.connect() as conn:
        buf = io.BytesIO(b"id,name\n1,Ada\n")
        await copyinsert_csv(buf, "people", "people_tmp", conn, schema="public", headers=True)
        await conn.commit()
    await engine.dispose()


# asyncio.run(load_once("postgresql+asyncpg://user:pass@host/db"))
```

For **async** DataFrames, use `pgcopyinsert.asynchronous.pd` and `pgcopyinsert.asynchronous.pl`.

---

## Development

```sh
git clone https://github.com/eddiethedean/pgcopyinsert.git
cd pgcopyinsert
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
ruff check src tests && ruff format --check src tests
mypy src/pgcopyinsert
pytest
```

---

## Links

- **Repository:** [github.com/eddiethedean/pgcopyinsert](https://github.com/eddiethedean/pgcopyinsert)
- **PyPI:** [pypi.org/project/pgcopyinsert](https://pypi.org/project/pgcopyinsert/)

License: **MIT** (see [LICENSE](LICENSE)).
