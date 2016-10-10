from collections import namedtuple, OrderedDict
from itertools import groupby
from ..timestamped_value import TimestampedValue
import operator

class Column:
	def __init__(self, results, field):
		self.results = results
		self.field = field

	def __len__(self):
		return len(self.results)

	def __getitem__(self, index):
		if isinstance(index, slice):
			return Column(self.results[index], self.field)
		return self.results[index][self.field]

	def __iter__(self):
		for row in self.results:
			yield row[self.field]

	def __reversed__(self):
		for row in reversed(self.results):
			yield row[self.field]

class Ops:
	def avg(data):
		# NOTE: Assumes the function is defined on the whole period of each row.
		return Ops.sum(data) / len(data)

	def at_start(data):
		return data[0]

	def at_end(data):
		return data[-1]

	def earliest(data):
		for val in data:
			if val is not None:
				return val
		return None

	def latest(data):
		for val in reversed(data):
			if val is not None:
				return val
		return None

	def _add(a, b):
		try:
			return a.union(b)
		except:
			return a + b

	def sum(data):
		ret = None
		for val in data:
			if val is None:
				continue
			if ret is None:
				ret = val
				continue
			ret = Ops._add(ret, val)
		return ret

	def union(data):
		return Ops.sum(data)

	def min(data):
		found = None
		for val in data:
			if found is None or (val is not None and val < found):
				found = val
		return found

	def max(data):
		found = None
		for val in data:
			if found is None or (val is not None and val > found):
				found = val
		return found

def aggregate(input_data): # input_data is a list of AggregatedFields.
	if len(input_data) == 1:
		# This is a feature, not only an optimization.
		# We can't usually sum averages, but we can do it if there is only one row!
		return input_data[0]

	AggregatedFields = type(input_data[0])

	results = []

	col = 0
	for field in AggregatedFields.internal_fields.values():
		ops = field.ops
		if ops is None:
			ops = [None]

		for op in ops:
			full_name = field.name + ("_" + op if op else "")
			spec = AggregatedFields.specs[full_name]

			if not op:
				op = "sum"

			if spec.expr:
				# Calculated later.
				res = None
			else:
				res = getattr(Ops, op)(Column(input_data, col))

			results.append(res)
			col += 1

	for col, (name, spec) in enumerate(AggregatedFields.specs.items()):
		if spec.expr:
			binary_ops = {
				"+": operator.add,
				"-": operator.sub,
				"*": operator.mul,
				"//": operator.floordiv,
				"/": operator.truediv,
			}

			for binop in binary_ops:
				if binop in spec.expr:
					op = binop
					break

			a_name, b_name = (x.strip() for x in spec.expr.split(op))
			a = results[AggregatedFields.specs[a_name].col]
			b = results[AggregatedFields.specs[b_name].col]
			if isinstance(a, TimestampedValue):
				a = a.value
			if isinstance(b, TimestampedValue):
				b = b.value
			try:
				results[col] = binary_ops[op](a, b)
			except (ZeroDivisionError, TypeError):
				results[col] = None

	return AggregatedFields(*results)

def aggregate_results(iterable):
	# iterable: period, AggregatedRow
	key = lambda x: x[1].tags
	for tags, rows in groupby(sorted(iterable, key=key), key):
		rows = list(rows)
		AggregatedRow = type(rows[0][1])
		result = aggregate([data.fields for p, data in rows])
		yield AggregatedRow(tags, result)
