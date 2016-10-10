from .._types import is_type

class Field:
	__slots__ = ("name", "type")

	def __init__(self, name, type=None):
		# Duck Type my ass
		assert(isinstance(name, str))
		if type is not None:
			assert(is_type(type))

		self.name = name
		self.type = type

	def __repr__(self):
		return "{} ({})".format(self.name, self.type)

class PartitionedListSchema:
	__slots__ = ("PartID", "Row", "sort_by")

	def __init__(self, PartID, Row, sort_by):
		# Duck Type my ass
		assert(is_type(PartID))
		assert(isinstance(sort_by, list))
		for x in sort_by:
			assert(isinstance(x, Field))

		self.PartID = PartID
		self.Row = Row
		self.sort_by = sort_by

"""
PartitionedList = [
	(PartID(), [
		(Field#0.type(), ...),
		...
	]),
	...
]
"""
