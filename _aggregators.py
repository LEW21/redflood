import itertools
import operator

"""
All mappings are single-pass, and iterate like dict.items().
data:
	((), channel, schema):
		(): channels
	((), game, schema):
		(): games
	((), lang, schema):
		(): langs
	((game,), lang, schema):
		(LoL,): langs
		(DotA,): langs
	((game,), channel, schema):
		(LoL,): channels
		(DotA,): channels
	((lang,), game, schema):
		(en,): games
	((lang,), channel, schema):
		(en,): channels
	((game, lang), channel, schema):
		(LoL, en): channels
		(DotA, en): channels
"""

def partitioned(data, key):
	return [(x, list(y)) for x, y in itertools.groupby(sorted(data, key=key), key=key)]

def powerset(iterable):
	"powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
	s = list(iterable)
	return itertools.chain.from_iterable(itertools.combinations(s, r) for r in range(len(s)+1))

class RawAggregator:
	def __init__(self, table, period):
		self.table = table
		self.period = period

	def rows(self, kind, rows):
		for tags, fields in rows:
			data = fields(self.period)
			for val in data:
				if val is None:
					break
			else:
				yield self.table.schema.kinds[kind].AggregatedRow(tags, data)

	def partitions(self, part_keys, kind):
		partitioned_data = self.table.query_raw(self.period, {}, part_keys, kind = kind) # [(part_id), [Tags(), MagicResult()]]
		for part_values, rows in partitioned_data:
			print("PV", part_values)
			yield part_values, self.rows(kind, rows)

	def __iter__(self):
		for part_keys in powerset(self.table.schema.group.by):
			for kind in self.table.schema.kinds:
				if not kind in part_keys:
					yield (part_keys, kind), self.partitions(part_keys, kind)

class ReAggregator:
	def __init__(self, table, source_periods):
		self.table = table
		self.source_periods = list(source_periods)

	def partitions(self, part_keys, kind):
		source_data = itertools.chain.from_iterable(
			((subperiod, part_values, list(rows)) for part_values, rows in self.table.aggregated_data.list_all_parts(subperiod, part_keys, kind, self.table.schema.default_sort_by))
			for subperiod in self.source_periods
		)

		for part_values, spr in partitioned(source_data, key=operator.itemgetter(1)):
			print("PV", part_values)
			big_rows = itertools.chain.from_iterable(
				((subperiod, row) for row in rows)
				for subperiod, _, rows in spr
			)
			yield part_values, self.table.aggregate_results(big_rows)

	def __iter__(self):
		for part_keys in powerset(self.table.schema.group.by):
			for kind in self.table.schema.kinds:
				if not kind in part_keys:
					yield (part_keys, kind), self.partitions(part_keys, kind)
