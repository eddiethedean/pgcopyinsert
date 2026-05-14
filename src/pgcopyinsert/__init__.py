from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _package_version

from fullmetalcopy.synchronous.copycsv import copy_from_csv

from pgcopyinsert.copyinsert import copyinsert_csv

try:
    __version__ = _package_version("pgcopyinsert")
except PackageNotFoundError:
    __version__ = "0.3.0"

__all__ = ["__version__", "copy_from_csv", "copyinsert_csv"]
