import itertools
import functools
import operator
import logging
from io import BytesIO

from .hashtable import HashTableType
from .serialization import SerializationMixin
from .path import PartPath
from .parts import Part, PartHeader, PartsReader

from ..list_ir import PartitionedListSchema

class PartPaths:
	__slots__ = ('sortindex', 'data')
	def __init__(self, sortindex, data):
		self.sortindex = sortindex
		self.data = data

class ListStore(SerializationMixin):
	def __init__(self, fs, prefix, schema):
		assert(isinstance(prefix, str))
		assert(isinstance(schema, PartitionedListSchema))

		self._fs = fs
		self._prefix = prefix
		self._schema = schema

	def _put(self, path, data):
		try:
			if path.range:
				raise ValueError("Cannot put parts of a file.")
			path = path.path
		except AttributeError:
			pass
		self._fs.put(path, data)

	def _get(self, path):
		logger = logging.getLogger(__name__)
		logger.info("GET %s", path)
		try:
			path.path
		except AttributeError:
			path = PartPath(path)

		return self._fs.get(path)

	def _get_cached(self, path):
		logger = logging.getLogger(__name__)
		logger.info("GET %s", path)
		try:
			path.path
		except AttributeError:
			path = PartPath(path)

		return self._fs.get_cached(path)

	def _base_url(self, sort_key):
		return "{prefix}/by_{sort}".format(prefix=self._prefix, sort=sort_key)

	@property
	def _PartIndex_HashTable(self):
		return HashTableType(self._schema.PartID, PartHeader)

	def _urls(self, part_id, sort):
		logger = logging.getLogger(__name__)

		base = self._base_url(sort)

		partindex = base + ".partindex"
		parts = base + ".parts"

		partindex_data = self._get_cached(partindex)
		logger.info("Partindex loaded.")

		ht = self._PartIndex_HashTable.from_bytes(partindex_data)
		part = ht[part_id]
		return PartPaths(
			sortindex = PartPath(parts, (part.sortindex_start, part.sortindex_end)),
			data = PartPath(parts, (part.data_start, part.data_end)),
		)

	def list_all_parts(self, sort):
		assert(sort in (x.name for x in self._schema.sort_by))

		base = self._base_url(sort)
		with self._get(base + ".parts") as f:
			yield from ((p.id, self._unserialize(BytesIO(p.data))) for p in PartsReader(f))

	def list(self, part_id, sort, range=None):
		assert(self._schema.PartID(part_id) or True)
		assert(sort in (x.name for x in self._schema.sort_by))

		urls = self._urls(part_id, sort)

		if range is None:
			return self._unserialize(self._get(urls.data))

		sortindex = self._get(urls.sortindex)
		# TODO find range in sortindex, and then set urls.data = urls.data[translated range]
		data = self._unserialize(self._get(urls.data))
		return (row for row in data if getattr(row, sort) in range)

	def _sort_serialize_rows(self, rows, sort_key):
		sorted_data = sorted(rows, key=lambda x: getattr(x.fields, sort_key.name), reverse=True)
		serialized_data = self._serialize(sorted_data)
		sortindex = b"" # TODO build sortindex
		return (sortindex, serialized_data)

	def save(self, partitions):
		# assert(partitions matches SinglePassPartitionedList concept)

		partitions = [(part_id, list(rows)) for part_id, rows in partitions]

		for sort_key in self._schema.sort_by:
			parts = bytearray()
			partindex = self._PartIndex_HashTable(capacity=len(partitions))

			for part_id, rows in partitions:
				if len(rows) < 1:
					continue
				p = Part(len(parts))
				p.id = part_id
				p.sortindex, p.data = self._sort_serialize_rows(rows, sort_key)

				parts += bytes(p)
				partindex[part_id] = p.header

			base = self._base_url(sort_key.name)
			self._put(base + ".parts", parts)
			self._put(base + ".partindex", partindex.data)

if __name__ == "__main__":
	from .. import Period
	from .._types import Tuple, Text
	from ..list_ir import Field, PartitionedListSchema
	from .fakefs import FakeFS

	game = Field(1, "game", Text(40, 'utf-8'))
	lang = Field(2, "lang", Text(5, 'utf-8'))
	viewers_avg = Field(5, "viewers_avg")
	viewers_max = Field(6, "viewers_max")

	schema = PartitionedListSchema(Tuple(game.type), [viewers_avg, viewers_max])
	ls = ListStore(FakeFS(), "xyz", schema)

	ls.save([
		(("LoL",), [
			["imaqtpie", None, ["en"], True, False, 1000, 3000, "2016-01-01T10:00:00Z"],
		]),
		(("DotA",), [
			["imaqtpie", None, ["en"], True, False, 1000, 2000, "2016-01-01T10:00:00Z"],
		]),
	])

	print(list(ls.list(("LoL",), "viewers_avg")))
	print(list(ls.list(("LoL",), "viewers_max")))
	print(list(ls.list(("DotA",), "viewers_avg")))

	for part_id, rows in ls.list_all_parts("viewers_avg"):
		print(part_id, list(rows))
