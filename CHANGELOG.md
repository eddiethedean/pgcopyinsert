# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] — 2026-05-14

### Added

- PEP 621 packaging (`pyproject.toml`), Ruff, Mypy, and GitHub Actions CI (Python 3.10–3.13).
- `py.typed` marker for type checkers.
- Tests for copyinsert orchestration, exports, insert/temp SQL compilation, async paths, Pandas/Polars wrappers, and CSV writing.
- **`constraint`** and **`null`** parameters on Pandas/Polars sync and async dataframe helpers, forwarded to `copyinsert_csv`.
- Sync Polars entrypoint **`copyinsert_polars`**; **`copyinsert_dataframe`** in `pgcopyinsert.pl` deprecated with `DeprecationWarning`.
- **`ValueError`** when `create_temp_table_from_table(..., columns=[])` (empty list).

### Changed

- **`fullmetalcopy`** imports use `synchronous.copycsv` / `asynchronous.copycsv` (compatible with fullmetalcopy ≥ 0.2.0).
- **`insert_from_table_stmt_ocdu`** now supplies a non-empty `set_` map for SQLAlchemy 2 / PostgreSQL `ON CONFLICT DO UPDATE`.
- Sync and async **`copyinsert_csv`** pass **`schema`** into `copy_from_csv`; async Pandas path aligns **`headers`** with sync.
- README overhaul: badges, TOC, API tables, examples (binary CSV, `engine.begin()`, constraint naming), development section.

### Removed

- Legacy `setup.py` / `setup.cfg` / `requirements.txt` (use `pip install -e ".[dev]"` for development).
- Unused third parameter on **`insert_from_table_stmt`** (previously ignored `constraint`).

### Fixed

- Tracked `*.egg-info` removed from version control; ignore patterns updated.

[0.3.0]: https://github.com/eddiethedean/pgcopyinsert/releases
