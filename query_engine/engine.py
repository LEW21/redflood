from itertools import groupby, chain
from ..period import Period
from ..window import max_window
from .expr import parse_expr, CallExpr, OverExpr, NameExpr, DifferentialExpr, ConstExpr
from .column import RawColumn, DTDifferential, DFieldDifferential
from .multiple import MultipleFunctions
from collections import OrderedDict, namedtuple
import warnings
import logging

def partitioned(data, key):
	return [(x, list(y)) for x, y in groupby(sorted(data, key=key), key=key)]

class FieldsBase:
	@classmethod
	def transform(Fields, get_source_field):
		result = OrderedDict()

		def _eval(expr):
			if isinstance(expr, NameExpr):
				if expr.name in result:
					return result[expr.name]
				else:
					return get_source_field(expr.name)

			elif isinstance(expr, OverExpr):
				return _eval(expr.expr).over(expr.over)

			elif isinstance(expr, CallExpr):
				args = (_eval(arg) for arg in expr.args)
				return getattr(next(args), expr.name)(*args)

			elif isinstance(expr, DifferentialExpr):
				if expr.variable == NameExpr("t"):
					return DTDifferential(_eval(expr.func))
				else:
					assert(expr.func == ConstExpr(1))
					return DFieldDifferential(_eval(expr.variable))
			else:
				raise TypeError("Unsupported expression: " + type(expr).__name__)

		for field in Fields.specs.values():
			result[field.name] = _eval(field.expr)

		return Fields(*result.values())

	def __call__(self, period, granularity = None):
		"""Aggregate data from the given period"""
		if not isinstance(period, Period):
			raise TypeError("period has to be of Period type")

		if granularity:
			return [(p, self(p)) for p in period.subperiods(granularity)]

		Fields = type(self)

		aggregated_fields = []

		for fieldspec in Fields.specs.values():
			value = getattr(self, fieldspec.name)

			for op in fieldspec.ops.values():
				if op:
					aggregated_fields.append(getattr(value, op)(period))
				else:
					aggregated_fields.append(value(period))

		return Fields.AggregatedFields(*aggregated_fields)

def FieldsType(specs_, AggregatedFields_):
	class Fields(namedtuple("FieldsData", tuple(field.name for field in specs_.values())), FieldsBase):
		specs = specs_
		AggregatedFields = AggregatedFields_
	return Fields

class QueryEngine:
	def __init__(self, schema):
		self.schema = schema

		self.largest_window = max_window([field.expr.max_window for field in self.schema.fields.values()])

		self.Fields = FieldsType(self.schema.fields, self.schema.AggregatedFields)
		self.GFields = FieldsType(self.schema.group.fields, self.schema.group.AggregatedFields)

	def required_period(self, requested_period):
		return Period(requested_period.start - self.largest_window.prev, requested_period.end + self.largest_window.next)

	def transform_datapoints(self, raw):
		raw = list(raw)
		return self.Fields.transform(lambda fname: RawColumn(raw, fname))

	def merge(self, series):
		series = list(series)
		return self.GFields.transform(lambda fname: MultipleFunctions(getattr(fields, fname) for tags, fields in series))

	@property
	def empty_result(self):
		return self.transform_datapoints([])
