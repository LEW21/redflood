import logging
import mmh3

def next_power_of_2(x):
	return 1<<(x-1).bit_length()

def HashTableType(key_type, val_type, hash_func = mmh3.hash, capacity_factor = 1.3):
	class HashTable:
		KEY_TYPE = key_type
		KEY_SIZE = KEY_TYPE.SIZE
		VAL_TYPE = val_type
		VAL_SIZE = VAL_TYPE.SIZE
		ROW_SIZE = KEY_SIZE + VAL_SIZE
		EMPTY_KEY = b'\x00' * KEY_SIZE
		CAPACITY_FACTOR = capacity_factor

		HASH = hash_func

		def __init__(self, capacity=None, _data=None):
			if not _data:
				assert(isinstance(capacity, int))

			self.capacity = capacity

			if _data:
				self.data = bytearray(_data)
			else:
				if capacity == 0:
					self.data = bytearray()
				else:
					self.data = bytearray(next_power_of_2(int(self.CAPACITY_FACTOR * capacity)) * self.ROW_SIZE)
				self.size = 0

			self.hash_mask = len(self.data) // self.ROW_SIZE - 1

			self._logger = logging.getLogger(__name__)

		def __bytes__(self):
			return bytes(self.data)

		@classmethod
		def from_bytes(cls, data):
			return cls(_data=data)

		def hash(self, key):
			return (self.HASH(bytes(key)) & self.hash_mask) * self.ROW_SIZE

		def _find(self, i, key, _log_collision_key=None):
			myhash = self.hash(key)
			collision_num = 0
			while key == self.EMPTY_KEY or bytes(self.data[i : i + self.KEY_SIZE]) != self.EMPTY_KEY:
				if self.data[i : i + self.KEY_SIZE] == key:
					return i

				collision_num += 1
				if _log_collision_key is not None and collision_num > 50:
					self._logger.warning("Collision #%s: %s vs %s", collision_num, _log_collision_key.rstrip(b"\x00"), bytes(self.data[i : i + self.KEY_SIZE].rstrip(b"\x00")))

				i += self.KEY_SIZE + self.VAL_TYPE.SIZE
				if i == len(self.data):
					i = 0
			raise KeyError

		def find(self, key):
			key = bytes(self.KEY_TYPE(key))
			i = self.hash(key)
			return self._find(i, key)

		def __setitem__(self, key, val):
			if self.size >= self.capacity:
				raise MemoryError("You're over hash table's capacity.")

			key = bytes(self.KEY_TYPE(key))
			i = self.hash(key)
			i = self._find(i, self.EMPTY_KEY, _log_collision_key=key)

			self.size += 1
			self.data[i : i + self.KEY_SIZE] = key
			sval = bytes(self.VAL_TYPE(val))
			assert(len(sval) == self.VAL_TYPE.SIZE)
			self.data[i + self.KEY_SIZE : i + self.ROW_SIZE] = sval

		def __getitem__(self, key):
			i = self.find(key)
			return self.VAL_TYPE.from_bytes(self.data[i + self.KEY_SIZE : i + self.ROW_SIZE])

	return HashTable

if __name__ == "__main__":
	from .._types import int32, ByteArray

	HashTable = HashTableType(ByteArray(40), int32)

	h = HashTable(capacity=5)

	h[b"LoL"] = 500
	h[b"Dota"] = 600
	h[b"SC"] = 700
	h[b"Counter-Strike: Global Offensive"] = 800
	h[b"D"] = 800

	assert(h[b"LoL"] == 500)
	assert(h[b"Dota"] == 600)
	assert(h[b"SC"] == 700)
	assert(h[b"Counter-Strike: Global Offensive"] == 800)
	assert(h[b"D"] == 800)

	try:
		h[b"Lorem Ipsum"]
		assert(False)
	except KeyError:
		pass

	try:
		h[b"Lorem Ipsum"] = 666
		assert(False)
	except MemoryError:
		pass

	print("OK")

if __name__ == "__main__":
	import json
	with open("games.json", "r") as f:
		games = json.load(f)

	HashTable = HashTableType(ByteArray(40), int32)
	h = HashTable(capacity=len(games))

	for game in games:
		h[game.encode('utf-8')] = 1337

	assert(h[b"Jump \'n Bump"] == 1337)
	assert(h[b'osu!'] == 1337)

	print("OK")
