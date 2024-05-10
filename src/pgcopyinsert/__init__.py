__version__ = '0.1.0'

from pgcopyinsert.synchronous.copy import copy_from_csv
from pgcopyinsert.synchronous.copyinsert import copyinsert_csv
from pgcopyinsert.synchronous.pd import copyinsert_dataframe
from pgcopyinsert.synchronous.pl import copyinsert_polars