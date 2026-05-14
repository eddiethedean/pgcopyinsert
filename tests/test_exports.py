import re
from pathlib import Path

from fullmetalcopy.synchronous.copycsv import copy_from_csv as fmc_copy_from_csv

import pgcopyinsert


def _pyproject_version() -> str:
    root = Path(__file__).resolve().parents[1]
    text = (root / "pyproject.toml").read_text(encoding="utf-8")
    match = re.search(r'^version = "([^"]+)"', text, re.MULTILINE)
    assert match is not None
    return match.group(1)


def test_reexports_copy_from_csv() -> None:
    assert pgcopyinsert.copy_from_csv is fmc_copy_from_csv


def test_version_matches_pyproject() -> None:
    assert pgcopyinsert.__version__ == _pyproject_version()
    assert isinstance(pgcopyinsert.__version__, str)
