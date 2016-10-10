from .timestamped_value import TimestampedValue
from .period import Period, to_datetime
from .window import Window, max_window
from .duration import Duration
from .range import Range
from .database import Database
from ._logging import get_rate_limited_logger, start_rate_limited_logger
from ._rollup import rollup

import os

def connect(schema_files = None, **default_options):
	db = Database(schema_files or os.getenv("REDFLOOD_SCHEMA_FILES", "").split(","))
	db.connect(**default_options)
	return db
