
def is_type(T):
	return hasattr(T, "SIZE") and hasattr(T, "from_bytes") and hasattr(T, "__bytes__")

def Sized(Type, size_, byte_order='big'):
	class Sized(Type):
		SIZE = size_

		def __bytes__(self):
			return self.to_bytes(self.SIZE, byte_order)

		@classmethod
		def from_bytes(cls, val):
			assert(len(val) == cls.SIZE)
			return super().from_bytes(val, byte_order)

	return Sized

int8 = Sized(int, 8)
int16 = Sized(int, 16)
int32 = Sized(int, 32)

def ByteArray(N):
	class ByteArray(bytes):
		__qualname__ = "ByteArray({})".format(N)
		SIZE = N
		__slots__ = ()

		def __new__(cls, data):
			return super().__new__(cls, data.ljust(cls.SIZE, b'\x00')[0:cls.SIZE])

		@classmethod
		def from_bytes(cls, val):
			assert(len(val) == cls.SIZE)
			return cls(val)
	return ByteArray

def Text(N, encoding='utf-8'):
	class Text(str):
		__qualname__ = "Text({}, '{}')".format(N, encoding)
		SIZE = N

		def __init__(self, arg):
			assert(isinstance(arg, str))

		def __bytes__(self):
			return bytes(ByteArray(N)(self.encode(encoding)))

		@classmethod
		def from_bytes(cls, val):
			assert(len(val) == cls.SIZE)
			return cls(val.rstrip(b'\0').decode(encoding))

	return Text

def Array(Type, N):
	class Array(tuple):
		SIZE = Type.SIZE * N
		__slots__ = ()

		def __new__(cls, *args):
			if len(args) == 1:
				return super().__new__(cls, args[0])
			assert(len(args) == N)
			return super().__new__(cls, args)

		def __bytes__(self):
			return b"".join(bytes(Type(x)) for x in self)

		@classmethod
		def from_bytes(cls, val):
			assert(len(val) == cls.SIZE)
			step = len(val)//N
			return cls(Type.from_bytes(val[i:i+step]) for i in range(0, len(val), step))

	return Array

def Tuple(*Types):
	x = Types
	class Tuple(tuple):
		Types = x
		SIZE = sum(T.SIZE for T in Types)

		def __new__(cls, *args):
			if len(Types) == 0 and len(args) == 0:
				return super().__new__(cls)

			if isinstance(args[0], tuple) or isinstance(args[0], list):
				return cls(*args[0])
			assert(len(args) == len(Types))
			return super().__new__(cls, (T(arg) for T, arg in zip(Types, args)))

		def __bytes__(self):
			return b"".join(x.__bytes__() for x in self)

		@classmethod
		def from_bytes(cls, val):
			assert(len(val) == cls.SIZE)
			vals = []
			pos = 0
			for Type in Types:
				vals.append(Type.from_bytes(val[pos:pos+Type.SIZE]))
				pos += Type.SIZE
			return cls(*vals)
	return Tuple
