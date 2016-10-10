from io import BytesIO

class FakeFS:
	def __init__(self):
		self.data = {}

	def put(self, path, data):
		self.data[path] = bytes(data)

	def get_cached(self, path):
		data = self.data[path.path]
		if path.range:
			data = data[path.range[0]:path.range[1]]
		return data

	def get(self, path):
		return BytesIO(self.get_cached(path))
