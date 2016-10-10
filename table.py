from .schema import parse_table_schema, RawTableSchema
from .aggregation_engine import aggregate_results
from ._aggregated_data_store import AggregatedDataStore
from .duration import Duration
from .period import Period
from .range import Range
from .query_engine import QueryEngine
from collections import namedtuple
from functools import reduce
from datetime import datetime
import operator
import weakref
import logging
import warnings
import itertools
import functools

def quote_ident(ident):
	return '"' + ident + '"'

class Table:
	def __init__(self, db, name, **kwargs):
		self.db = weakref.ref(db)
		self.name = name

		self.schema = parse_table_schema(RawTableSchema(**kwargs))

		self.rollups = {Duration(freq): set(Duration(gran) for gran in grans) for freq, grans in self.schema.rollups.items()}
		self.granularities = reduce(operator.or_, (grans for grans in self.rollups.values()))

		self.query_engine = QueryEngine(self.schema)

		self.Tags = namedtuple("Tags", self.schema.tags.keys())
		self._tags_func = lambda row: self.Tags(*(getattr(row, tag) for tag in self.schema.tags.keys()))

	@property
	def since(self):
		return self.schema.since

	@property
	def raw_data(self):
		try:
			return self._raw_data
		except:
			self._raw_data = self.db().RawDataStore(self)
			return self._raw_data

	@property
	def aggregated_data(self):
		try:
			return self._aggregated_data
		except:
			self._aggregated_data = AggregatedDataStore(self.db().ListStore, self)
			return self._aggregated_data

	def wants_rollup(self, period):
		if period.start < self.since:
			return False

		for freq, grans in self.rollups.items():
			if period.duration in grans:
				if ((period.start - datetime(2016, 1, 1)) % freq).total_seconds() == 0:
					return True

		return False

	def wanted_rollups(self, end):
		wanted = []
		for freq, grans in self.rollups.items():
			if ((end - datetime(2016, 1, 1)) % freq).total_seconds() == 0:
				for gran in grans:
					p = Period(duration=gran, end=end)
					if p.start >= self.since:
						wanted.append(p)
		return sorted(wanted, key=lambda x: x.duration)

	def rollup_status(self, period):
		return 'done'
		res = iter(self.db()._autorollup_status_db.execute("SELECT status FROM rollups WHERE what = %s and start = %s and end = %s", (self.name, period.start, period.end)))
		try:
			return next(res)[0]
		except StopIteration:
			if self.wants_rollup(period):
				return "toplan"

			return "invalid"

	def request_rollup(self, period):
		if self.wants_rollup(period):
			return

		if period.start < self.since:
			raise ValueError("We don't have data from requested period.")

		if period.end > (datetime.utcnow() - timedelta(minutes=5)):
			raise RuntimeError("We're still collecting the data from given period.")

		# Needs a lower priority than all the automatically scheduled rollups.
		self.db()._autorollup_status_db.execute("INSERT INTO rollups (what, start, end, status, priority) VALUES (%s, %s, %s, 'todo', %s)", self.name, period.start, period.end, -10*365*24*3600)

	def get(self, where, period, granularity = None):
		kind = None
		for kindspec in self.schema.kinds.values():
			if (set(where.keys()) == set(kindspec.Tags._fields)):
				kind = kindspec.name

		assert(kind)

		if granularity is None:
			granularity = period.duration

		logger = logging.getLogger(__name__)
		logger.info("Calling query_raw()")
		results = self.query_raw(period, where, kind = kind)
		try:
			part_id, part_data = next(results)
			rtags, result = next(part_data)
		except:
			rtags = self.Tags(**where)
			result = self.query_engine.empty_result

		logger.info("Iterating over subperiods")
		for subperiod, fields in result(period, granularity):
			yield subperiod, self.schema.kinds[kind].AggregatedRow(rtags, fields)
		logger.info("Iteration done")

	def aggregate_results(self, results):
		return aggregate_results(results)

	def list(self, period, partition, kind, sort, range=None):
		return self.aggregated_data.list(period, partition, kind, sort, Range.cast(range) if range is not None else None)

	def _transform_series(self, data):
		for tags, datapoints in itertools.groupby(data, key=self._tags_func):
			#try:
			result = self.query_engine.transform_datapoints(datapoints)
			#except ValueError:
			#	continue
			yield tags, result

	def _merge_series(self, merge_by_func, data):
		for tags, subseries_data in itertools.groupby(data, key=merge_by_func):
			yield tags, self.query_engine.merge(self._transform_series(subseries_data))

	def query_raw(self, period, where = {}, partition_by = (), kind = None):
		if not kind:
			kind = self.schema.kind

		merge_by = kind if kind != self.schema.kind else None

		period = self.query_engine.required_period(period)

		res = self.raw_data.query(period, where)

		sort_by = partition_by + ((merge_by,) if merge_by else tuple()) + tuple(self.schema.tags.keys())
		sort_by_func = lambda row: tuple(getattr(row, field) for field in sort_by)

		res = sorted(res, key=sort_by_func)

		series_func = self._transform_series
		if merge_by:
			series_func = functools.partial(self._merge_series, lambda row: self.schema.kinds[merge_by].Tags(getattr(row, merge_by)))

		if partition_by:
			partition_by_func = lambda row: tuple(getattr(row, field) for field in partition_by)
		else:
			partition_by_func = lambda row: ()

		with warnings.catch_warnings():
			warnings.simplefilter("ignore", UserWarning) # Ignore scipy.interpolate warning about not good-enough interpolation.

			for part_id, rows in itertools.groupby(res, key=partition_by_func):
				yield part_id, series_func(rows)

	# TODO Delete this code
	def cql_raw_setup(self, drop_first=False):
		yield from self.raw_data.cql_setup(drop_first=drop_first)
