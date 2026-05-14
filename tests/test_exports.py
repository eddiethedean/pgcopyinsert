from fullmetalcopy.synchronous.copycsv import copy_from_csv as fmc_copy_from_csv

import pgcopyinsert


def test_reexports_copy_from_csv() -> None:
    assert pgcopyinsert.copy_from_csv is fmc_copy_from_csv


def test_version_is_defined() -> None:
    assert isinstance(pgcopyinsert.__version__, str)
    assert pgcopyinsert.__version__
