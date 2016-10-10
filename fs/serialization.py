import msgpack

class SerializationMixin:
	def _serialize(self, rows):
		return b"".join(msgpack.packb(row.serialize()) for row in rows)

	def _unserialize(self, stream):
		return (self._schema.Row.unserialize(x) for x in msgpack.Unpacker(stream, encoding='utf-8'))
