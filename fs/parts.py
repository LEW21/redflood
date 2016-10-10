from .._types import Array, int32

"""
parts file:
[sortindex start, data start, data end]
id
sortindex
data
[sortindex start, data start, data end]
id
sortindex
data
...
"""

class PartHeader(Array(int32, 3)):
	__slots__ = ()

	@property
	def id_end(self):
		return self[0]

	@property
	def sortindex_start(self):
		return self[0]

	@property
	def sortindex_end(self):
		return self[1]

	@property
	def data_start(self):
		return self[1]

	@property
	def data_end(self):
		return self[2]

class Part:
	def __init__(self, startpos):
		self.start = startpos

	@classmethod
	def load(Part, f):
		part = Part(f.tell())
		header_bytes = f.read(PartHeader.SIZE)
		if header_bytes == b"":
			raise StopIteration
		header = PartHeader.from_bytes(header_bytes)
		part._id = f.read(header.sortindex_start - f.tell())
		part.sortindex = f.read(header.sortindex_end - f.tell())
		part.data = f.read(header.data_end - f.tell())
		return part

	@property
	def id(self):
		return tuple(x.decode("utf-8") for x in self._id.split(b"\0"))[:-1]

	@id.setter
	def id(self, values):
		if values:
			self._id = b"\0".join(x.encode("utf-8") for x in values) + b"\0" # null terminated strings <3
		else:
			self._id = b""

	@property
	def header(self):
		header_start = self.start
		id_start = header_start + PartHeader.SIZE
		sortindex_start = id_start + len(self._id)
		data_start = sortindex_start + len(self.sortindex)
		end = data_start + len(self.data)

		return PartHeader(sortindex_start, data_start, end)

	def __bytes__(self):
		return bytes(self.header) + self._id + self.sortindex + self.data

class PartsReader:
	def __init__(self, f):
		self.f = f

	def __iter__(self):
		return self

	def __next__(self):
		return Part.load(self.f)
