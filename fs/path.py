
class PartPath:
	def __init__(self, path, range=None):
		self.path = path
		self.range = range

	def __getitem__(self, range):
		if not self.range:
			self.range = range
		else:
			# Subrange
			raise NotImplementedError

	def __repr__(self):
		if not self.range:
			return self.path
		else:
			return "{}#{}-{}".format(self.path, self.range[0], self.range[1])
