from datetime import datetime, timedelta
from .duration import Duration
from .period import Period, to_datetime
from .timestamped_value import TimestampedValue
from collections import OrderedDict

def to_json_ready(obj):
	if isinstance(obj, datetime):
		return obj.isoformat() + "Z"
	if isinstance(obj, timedelta):
		return obj.total_seconds()
	if isinstance(obj, Period):
		return to_json_ready(period_to_dict(obj))
	if isinstance(obj, tuple) and hasattr(obj, '_asdict'):
		return to_json_ready(obj._asdict())
	if isinstance(obj, OrderedDict):
		return OrderedDict((name, to_json_ready(value)) for name, value in obj.items())
	if isinstance(obj, dict):
		return {name: to_json_ready(value) for name, value in obj.items()}
	if isinstance(obj, list) or isinstance(obj, tuple) or isinstance(obj, set):
		return [to_json_ready(val) for val in obj]
	return obj

class AggregatedRowBase:
	def __init__(self, tags, fields):
		self.tags = tags
		self.fields = fields

	def __getitem__(self, i):
		if i == 0:
			return self.tags
		if i == 1:
			return self.fields
		raise IndexError

	@classmethod
	def serialized_column_indexes(AggregatedRow):
		by_name = {}
		i = 0
		for tag in AggregatedRow.Tags._fields:
			by_name[tag] = i
			i += 1

		for field in AggregatedRow.Fields.specs.values():
			if field.type[0] == "timestamped":
				by_name[field.name] = i
				i += 1
				by_name[field.name + ".time"] = i
				i += 1
			elif field.type[0] == "period":
				by_name[field.name + ".start"] = i
				i += 1
				by_name[field.name + ".end"] = i
				i += 1
			else:
				by_name[field.name] = i
				i += 1

		return by_name

	def serialize(self):
		AggregatedRow = type(self)

		row = []

		for tag in self.tags:
			row.append(tag)

		for val, field in zip(self.fields, AggregatedRow.AggregatedFields.specs.values()):
			if field.type[0] == "timestamped":
				if val is not None:
					row.append(val.value)
					row.append(val.time)
				else:
					row.append(None)
					row.append(None)
			elif field.type[0] == "period":
				if val is not None:
					row.append(val.start)
					row.append(val.end)
				else:
					row.append(None)
					row.append(None)
			elif field.type[0] == "duration":
				row.append(val.total_seconds())
			else:
				row.append(val)

		return to_json_ready(row)

	@classmethod
	def unserialize(AggregatedRow, row):
		tagnum = len(AggregatedRow.Tags._fields)
		tags = row[0:tagnum]
		i = tagnum

		fields = [None]*len(AggregatedRow.AggregatedFields.specs)
		f = 0
		for field in AggregatedRow.AggregatedFields.specs.values():
			val = row[i]
			i += 1
			if field.type[0] == "timestamped":
				val = TimestampedValue(val, to_datetime(row[i]))
				i += 1
			elif field.type[0] == "period":
				val = Period(val, row[i])
				i += 1
			elif field.type[0] == "duration":
				val = Duration(seconds=val)
			elif field.type[0].startswith("set<"):
				val = set(val)
			fields[f] = val
			f += 1

		return AggregatedRow(AggregatedRow.Tags(*tags), AggregatedRow.AggregatedFields(*fields))

def AggregatedRowForKind(kind_, Tags_, AggregatedFields_):
	class AggregatedRow(AggregatedRowBase):
		__name__ = "AggregatedRow:" + kind_

		kind = kind_
		Tags = Tags_
		AggregatedFields = AggregatedFields_

	return AggregatedRow
