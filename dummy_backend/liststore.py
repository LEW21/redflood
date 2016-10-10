from ..list_ir import PartitionedListSchema

class ListStore:
	def __init__(self, prefix, schema):
		assert(isinstance(prefix, str))
		assert(isinstance(schema, PartitionedListSchema))

		self._prefix = prefix
		self._schema = schema

	def save(self, partitions):
		# assert(partitions matches SinglePassPartitionedList concept)
		pass

	def list_all_parts(self, sort):
		assert(sort in (x.name for x in self._schema.sort_by))

		for part_values in whatever:
			yield part_values, self.list(period, dict(zip(part_keys, part_values)), self.schema._sort_by[0], limit=None)

	# TODO add official paging support
	def list(self, part_id, sort, range=None):
		assert(self._schema.PartID(part_id) or True)
		assert(sort in (x.name for x in self._schema.sort_by))

		for list_ir_row in whatever:
			yield list_ir_row
